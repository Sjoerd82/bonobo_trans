"""
TODO list:
 - Implement EXP and REX
 - Implement case (in)sensitivity, both in comparison and in dict keys (PEP455)
 - Issue a WARNING if a key already exists and will be overwritten.
 - Prevent "SAWarning: Textual column expression"
 - Use bind parameters in SQL Order By
 - Stop on warnings?
 - Decide if we want to support an SQL-override which is an SQLAlchemy Select
"""

import copy
import logging
import operator

from bonobo.config import Configurable, ContextProcessor, Option, Service
from bonobo.config import use_context
from bonobo.errors import UnrecoverableError
import pandas
from sqlalchemy import Column, Integer, Float, LargeBinary, MetaData, String, Table
from sqlalchemy import and_, column, desc, func, literal, literal_column, or_, select, text
from sqlalchemy.engine import create_engine
import sqlalchemy.sql

from bonobo_trans.logging import logger

@use_context
class DbLookup(Configurable):
	"""The DbLookup transformation looks up data in a SQL database.
	
	.. note::
		Database connectivity is provided by SQLAlchemy. The engine must be
		provided via a *Bonobo Service*, containing an 'sqlalchemy.engine'.
	
	.. admonition:: Configuration options

		**Required:**
		
			-	**comparison** *(list)*

		*Optional:*
		
			-	**table_name**         *(str)*
			-	**sql_override**       *(str or sqlalchemy.sql.Select)*
			-	**sql_override_cols**  *(list of sqlalchemy.sql.schema.Column)*
			-	**order_by**           *(str, int, dict, list of str/int/dict)*
			-	**ret_fields**         *(str, dict, list of str)*
			-	**ret_prefix**         *(str)*
			-	**name**               *(str)*
			-	**verbose_init**       *(bool)*
			-	**verbose_sql**	       *(bool)*
			-	**verbose_data**       *(bool)*
			-	**multiple_match**     *(int)*
			-	**caching**            *(int)*
		
	**Description of the options:**

		.. note::
			Not all options can be used together, generally, the transformation
			will issue a warning or error, depending on the severity, when invalid
			combinations are issued.
		
		.. note::
			The lookup transformation starts by loading the lookup data
			("source-data") in memory, unless the ``caching`` setting is set to
			LKP_CACHING_DISABLED. If disabled, the 'sql_*'-options will be ignored.
			
		table_name, sql_override
			If the ``sql_override`` option is used, the ``table_name`` option will be
			ignored. The ``sql_override`` can either be a SQL statement (str) or a
			SQLAlchemy sql object. The SQL-override provides for some flexibility,
			however it also has downsides. One of them is that the SQLAlchemy
			database reflection is not longer limited to one table, causing an
			error if there is an, otherwise unrelated, invalid view in the
			database.
		
		sql_override_cols	
			If an sql_override is provided as an SQL string (opposed to an
			SQLAlchemy Select object) we will not have a column list to create an
			SQLite memory table with. To enable caching using a SQLite memory
			table the ``sql_override_cols``-option can be used to provide this list
			of columns.
			
			This option must be a list of SQLAlchemy columns including datatype.
	
		order_by
			An ORDER BY is useful in a scenario where more than one row is expected
			and in conjunction with LKP_MM_FST or LKP_MM_LST. It may also be useful
			in conjunction with LKP_MM_LOV and LKP_MM_ALL to sort the output of
			multiple rows. The 'order_by' option can be specified as follows:
			
			* column position (int)
			* column name (str)
			* column name/position + direction (dict)
			* a list of any of the above, for example::
			
				[ 1,'user_id',{'user_type','DESC'},{6,'ASC'} ]
				
			.. caution::
				Don't specify more than one column in a single dictionary, as Python
				does not guarantee the order of dict keys.
		
		comparison
			The search comparison lies at the core of the lookup. It consists of a
			list of lists of dictionaries. Example::
			
				[ol1 [il1 {d1},{d2} ], [il2 {d3} ] ]
			
			The two 'inner lists' (il1 and il2), containing the dicts, are AND-
			grouped together. The conditions *within* the inner lists are OR-grouped
			together. The example lookup would be created as follows::
			
				WHERE (d1=X OR d2=Y) AND d3=Z
			
			Each condition is a dictionary containing a key-value pair where the
			key represents a column in the lookup table and the value the value to
			compare with. There are also "special keys", as described below.
			
			*	**Special key: _compare_with_**
		
				The condition value can be of three different types:
				
				===========  ==================================================
				value        Description
				-----------  --------------------------------------------------
				LKP_VAL_COL  The 'value' is actually a column name available in the d_row (default)
				LKP_VAL_VAL  The 'value' is hardcoded (default fallback, if 'value' is not a valid column)
				LKP_VAL_EXP	 The 'value' is an expression [not implemented]
				LKP_VAL_REX  The 'value' is a regular expression [not implemented]
				===========  ==================================================
			
			*	**Special key: _operator_**
				
				By default the comparison operator is an 'equal to', but a different
				operator can be used by adding an _operator_ key, containing one of the
				following (string) values:
				
				::
				
					== or !=				Equality
					<, >, <=, >= or <>		Larger or smaller
					in or not in			Sets
				
				
			*	**Special key: _case_sensitive_**
			
				(not implemented)
				
			
			More examples:
		
				Simple OR-comparison (note one group!)::

				  [{'target_field_name1':'matching_value', 'target_field_name2':'matching_value','_operator_':'<>'}]
				
				Doing a between::
				
				  [{'target_field_name1':'matching_value', '_operator_':'>'},{'target_field_name1':'matching_value','operator':'<'}]

				This will not work, because groups are AND-ed together, use IN instead::
				
				  [{'target_field_name1':'matching_value'},{'target_field_name1':'matching_value'}]
				
				Do this instead::
				
				  [{'target_field_name1':'(matching_value1, matching_value2)','operator':'IN'}}]
				
				AND-comparison (note two groups!)::
				
				  [{'target_field_name1':'matching_value'}
				  ,{'target_field_name2':'matching_value'}]
				
				Composite AND+OR-comparison::
				
				  [ {'target_field_name1':'matching_value'
				    ,'target_field_name2':'matching_value'}
				  , {'target_field_name3':'matching_value'}
				  , {'target_field_name4':'matching_value'}]
				  
				Multiple blocks result = ({1} OR {2}) AND {3} AND {4}
	
		ret_fields
			List of column(s) to return from lookup table. If not specified, all
			columns will be returned. Use a dictionary to output an alias, for
			example: {'lkp_user_id','user_id'} will return a dict with the key
			'lkp_user_id' (instead of 'user_id') and value of 'user_id'.
			
			It is allowed to return the same value multiple times, for example::
			
			  {'user_id_1':'user_id', 'user_id_2':'user_id'}
			
			The 'ret_fields' option can be specified as follows:
			
			- None (return all fields)
			- column name (str)
			- alias(es) + column name(s) (dict, key=alias, val=source column)
			- a list of any of the above, for example::
			
				[ 'user_id', {'user_type_A','user_type'}, {'user_type_B':'user_type', 'user_description','user_desc'} ]
		
		column_prefix
			Prefix to apply to output column keys. For example: 'lkp_users' would
			output column 'user_id' as 'lkp_users.user_id'. Note the separation dot.
	
		name
			If this option is specified, an extra special key will be added to the
			output, called __LKP_<name>__. This key is a dict containing misc.
			lookup details.
			
			================  =================================================
			CACHING	          Caching strategy
			----------------  -------------------------------------------------
			MULTIPLE_MATCH    Multiple match setting
			LKP_ROWS_SOURCE   Number of rows in the source-data
			LKP_ROWS_MATCHED  Number of rows matched (before selecting first or last)
			LKP_ROW_NR        Row index, in case of LKP_MM_ALL
			================  =================================================
	
		verbose_init
			If True, all interesting details are printed to	the output.
			
		verbose_sql
			If True, the generated SQL statement for the source data and
			lookups will be printed.

		verbose_data
			If True, every outgoing row is printed.

	.. note::
		**Notes on multiple match**
	
		The DbLookup can either return a single or multiple rows. However,
		often only one row is required or expected. This setting decides what
		happens when more than one row is received from the database.
		
		==========      =======================================================
		Setting         Result
		----------      -------------------------------------------------------
		LKP_MM_ALL 		Return all rows, this will generate additional rows in the Bonobo chain!
		LKP_MM_LOV		Return all rows as one list (List of Values)
		LKP_MM_ANY		Return first received row
		LKP_MM_FST		Return first row (more efficient than return last)
		LKP_MM_LST		Return last row (use first instead, if possible)
		LKP_MM_ERR		Raise error
		==========      =======================================================
	
	
	.. note::
		**Notes on caching**
	
		The DbLookup first pulls in the table on which to perform the lookup.
		It stores this in one of the data structures below.

		*	**None / Disabled**			
			Don't cache data. Do a query on the lookup table for every row passing
			through the lookup. Useful when the lookup table data changes during
			processing. You cannot specify any "source-data" 'sql_*'-options.
		*	**SQLite memory table**
			Requires datatypes, thus cannot be used in combination with a 'raw'
			SQL statement.
		*	**Pandas DataFrame**
			
			SQLite vs Pandas:
			https://github.com/thedataincubator/data-science-blogs/blob/master/sqlite-vs-pandas.md
			
			Caching can be enabled via the 'caching'-option. It accepts the
			following values:
			
			======================    =========================
			``caching``               Description
			----------------------    -------------------------
			LKP_CACHING_DISABLED      Don't use caching
			LKP_CACHING_ENABLED       Will select the default
			LKP_CACHING_SQLITE_MEM    Force SQLite memory table
			LKP_CACHING_PANDAS        Force Pandas DataFrame
			======================    =========================
			
			.. tip::
				To improve performance and reduce memory usage, always limit the source
				selection as much as possible using the ``ret_fields`` (limit columns)
				option and a where clauses (limit rows) whenever possible.

	Todo:
		Issues, todo's and outstanding questions
	
		*	Q) How much data can we handle before starting to run into problems?
		       Consider implementing *Dask*.
		*	Q) What problems can we expect when the source data is big?
		*	TODO) Cleaner import statements (what's the best practice?)
		*	TODO) Implement LKP_VAL_EXP and LKP_VAL_REX
	
	Args:
		*	**d_row_in** *(dict)*

		d_row_in is a dictonary containing row data. It must contain all
		columns specified in the 'comparison'.
		
	Returns:
		*	**d_row_out** *(dict)*
		
		d_row_out contains all the keys of the incoming	dictionary plus keys
		(columns) specified in 'ret_fields' (or all columns if not specified).
		Any keys in the incoming record that are identical to return columns
		are overwritten.
		
		If 'multiple_match' = LKP_MM_ALL, the transformation may yield more
		than one row.
	"""

	LKP_VAL_COL = 0				# Value is a column nmae
	LKP_VAL_VAL = 1				# Value is a value
	LKP_VAL_EXP = 2 			# Value is an expression (not implemented)
	LKP_VAL_REX = 3				# Value is an RegExp (not implemented)
	
	LKP_MM_ALL = 0				# Return all rows
	LKP_MM_ANY = 1				# Return any row (randomly)
	LKP_MM_FST = 2				# Return first row
	LKP_MM_LST = 3				# Return last row
	LKP_MM_LOV = 4  			# Return all rows as a list
	LKP_MM_ERR = 9				# Raise error
	
	LKP_CACHING_DISABLED   = 0	# Don't use caching
	LKP_CACHING_ENABLED    = 1	# Will select the default (sqlite-mem)
	LKP_CACHING_SQLITE_MEM = 2	# Caching enabled, Force SQLite memory
	LKP_CACHING_PANDAS     = 3	# Caching enabled, Force Pandas
	
	engine = Service('sqlalchemy.engine')
	comparison        = Option(required=True,  type=list)
	table_name	      = Option(required=False, type=str)
	ret_fields        = Option(required=False)
	ret_prefix	      = Option(required=False, type=str)
	sql_override      = Option(required=False, type=str)
	sql_override_cols = Option(required=False, type=list)
	order_by          = Option(required=False)
	name              = Option(required=False, type=str, default="untitled")
	multiple_match    = Option(required=False, type=int, default=LKP_MM_ANY)
	caching           = Option(required=False, type=int, default=LKP_CACHING_ENABLED)
	verbose_init      = Option(required=False, type=bool, default=False)
	verbose_sql	      = Option(required=False, type=bool, default=False)
	verbose_data      = Option(required=False, type=bool, default=False)
	
	def _get_oper_python(self, oper='eq'):
		"""Return a Python comparison operator."""
		if oper.lower() in ('<','lt'):			# less than
			return operator.lt
		elif oper.lower() in ('<=','=<','le'):	# less or equal
			return operator.le
		elif oper.lower() in ('=','==','eq'):	# equal
			return operator.eq
		elif oper.lower() in ('!=','=!','ne'):	# not equal
			return operator.ge
		elif oper.lower() in ('>=','=>','le'):	# greater or equal
			return operator.ge
		elif oper.lower() in ('>','gt'):		# greater than
			return operator.gt
		elif oper.lower() in ('in','contains'):	# contains
			return operator.contains
		elif oper.lower() in ('is'):			# for comparison with None / NULL
			return operator.is_
		elif oper.lower() in ('is not'):		# for comparison with None / NULL
			return operator.is_not
			
	def _get_oper_pandas(self, oper='eq'):
		"""Return a pandas understandable comparison operator (string)."""
		if oper.lower() in ('<','lt'):			# less than
			return '<'
		elif oper.lower() in ('<=','=<','le'):	# less or equal
			return '<='
		elif oper.lower() in ('=','==','eq'):	# equal
			return '=='
		elif oper.lower() in ('!=','=!','ne'):	# not equal
			return '!='
		elif oper.lower() in ('>=','=>','le'):	# greater or equal
			return '>='
		elif oper.lower() in ('>','gt'):		# greater than
			return '>'
		elif oper.lower() in ('in','contains'):	# contains; pandas: == [...]
			return '=='
		elif oper.lower() in ('not in'):		# does not contain; pandas: != [...]
			return '!='
	
	def _get_sqlite_datatype(self, python_type):
		"""Convert Python data type to SQLAlchemy SQLite data type.
		
		SQLite v3 data types are:
			NULL		The value is a NULL value.
			INTEGER		The value is a signed integer, stored in 1,2, 3, 4, 6, or 8 bytes depending on the magnitude of the value.
			REAL		The value is a floating point value, stored asan 8-byte IEEE floating point number.
			TEXT		The value is a text string, stored using thedatabase encoding (UTF-8, UTF-16BE or UTF-16LE).
			BLOB		The value is a blob of data, stored exactly asit was input.
		
		Args:
			python_type (type)
			
		Returns:
			SQLAlchemy datatype
		"""
		if python_type is None:
			return None
		elif python_type is int or python_type is bool:
			return Integer()
		elif python_type is float:
			return Float()
		elif python_type is str:
			return String()
		else:
			logger.warning("[LKP_{}] Could not determine SQLite data type, using BLOB.".format(self.name))
			return LargeBinary()
	
	def _get_cols_from_comparison(self, comparison):
		"""Return unique list of column names used in comparison.
		
		Args:
			comparison (see class description)
			
		Returns:
			List of (unique) columns (str)
		"""
		columns = []
		for and_group in comparison:
			for cols in and_group:
				columns.extend(cols.keys())
				
		columns = list(set(columns))	# de-dup
		
		if '_operator_'     in columns:
			columns.remove('_operator_')
			
		if '_compare_with_' in columns:
			columns.remove('_compare_with_')
		
		return columns
	
	def _get_return_columns(self, ret_fields):
		"""Return dictionary of return columns.
		
		Creates dictionary containing all the return columns based on
		'ret_fields', which can contain a mixture of datatypes.
		
		Args:
			ret_fields (see description)
			
		Returns:
			dictionary
		"""
		
		if isinstance(ret_fields, str):
			return {ret_fields: ret_fields}

		elif isinstance(ret_fields, dict):
			return ret_fields
			
		elif isinstance(ret_fields, list):
			columns_d = {}
			for ret_col in ret_fields:
				if isinstance(ret_col, str):
					columns_d[ret_col] = ret_col
				elif isinstance(ret_col, dict):
					columns_d.update(ret_col)
					# TODO: check for uniqueness (?)
				else:
					raise UnrecoverableError("[LKP_{0}] ERROR: 'ret_fields': Invalid datatype, please use string, dict or list.".format(self.name))
			return columns_d
		else:
			raise UnrecoverableError("[LKP_{0}] ERROR: 'ret_fields': Invalid datatype, please use string, dict or list.".format(self.name))

	def _get_orderby_cols(self, order_by):
		"""Return list of "order by"-columns.
		
		The returned list if SQLAlchemy TextClause can be cast to a plain SQL
		string using str(), enabling usage in a text-based query.
		
		Args:
			'order_by'-option (see class description)
			
		Returns:
			List of sqlalchemy.sql.elements.TextClause
		"""
		# CAUTION: SQL-INJECTION RISK!
		# 	to MITIGATE use bound variables.

		if isinstance(order_by, (str,int,dict)):
			order_by = [order_by]
		
		if isinstance(order_by, list):
			order_by_cols = []
			for col in order_by:
				if isinstance(col, (int,str)):
					order_by_cols.append(text(str(col)))
				elif isinstance(col, dict):
					if len(col) > 1:
						logger.warning("[LKP_{}] 'order_by': It's not recommended to include more than one ORDER BY column into a dictionary, the order cannot be guaranteed.".format(self.name))
					for col, direction in col.items():
						if direction.upper() in ('ASC','ASCENDING'):
							order_by_cols.append(str(col))
						elif direction.upper() in ('DESC','DESCENDING'):
							order_by_cols.append(desc(str(col)))
						else:
							logger.error("[LKP_{0}] 'order_by': Sort direction for column '{1}' should be either ASC or DESC, got: '{2}.".format(self.name, col, direction))
				else:
					logger.error("[LKP_{0}] 'order_by': Ignoring ORDER BY on column '{}', due to invalid datetype. Must be either int, str or dict {'col':'desc'}.".format(self.name, col))
					
			return order_by_cols
				
		else:
			raise UnrecoverableError("[LKP_{}] ERROR: 'order_by' must be a list of column names (string) or list of column positions (int).".format(self.name))
			return
			
	def _append_where_clause(self, select, comparison):
		"""Append comparison as a where clause to the given SQLAlchemy select.
			
		Args:
			select      (sqlalchemy.sql.Select)
			comparison  (see class description)

		Returns:
			Where clause string (str)
		"""		
		for group_and in comparison:
			conditions_or = []
			for condition in group_and:			
			
				# default
				column_operator = operator.eq
				
				# get comparison settings
				for key, val in condition.items():
					if key == '_operator_':
						column_operator = self._get_oper_python(oper=val)
			
				# process conditions (sort by key to match bound params later)
				for key, val in sorted(condition.items()):
					if key in ('_operator_','_compare_with_') :
						pass
					else:
						#func.lower = for case insensitive search
						if val is None:
							conditions_or.append( column(key) == None )
						else:
							conditions_or.append( column_operator(func.lower(column(key)),func.lower(literal(val))) )	#literal because it generates a predictable param name
				
			# appended using AND!
			select = select.where(or_(*conditions_or))
		return select
		
	def _get_sql_binds(self, comparison, row=None):
		"""Return dictionary with bound variables.
		
		Args:
			comparison  (see class description)
			row 		(dict) optional row data, in case LKP_VAL_COL used.

		Returns:
			Dictionary with bind key-pair values.
		"""
		binds = {}
		cond_i = 1
		for group_and in comparison:
			conditions_or = []
			for condition in group_and:
			
				# default
				column_compare_with = self.LKP_VAL_COL
			
				# get comparison settings
				for key, val in condition.items():
					if key == '_compare_with_':
						if isinstance(val, int):
							column_compare_with = val
						else:
							raise UnrecoverableError("[LKP_{0}] ERROR: _compare_with_ key must be of int datatype, please use the constants.".format(self.name))
									
				# process conditions (sort by key)
				for key, val in sorted(condition.items()):
					if key in ('_operator_','_compare_with_') :
						pass
					else:
				
						# if LKP_VAL_COL, check if the column exists in row
						if column_compare_with == self.LKP_VAL_COL:
							if row is None:
								raise UnrecoverableError("[LKP_{0}] ERROR: column lookup specified, but no row was provided.".format(self.name))
							elif val not in row.keys():
								raise UnrecoverableError("[LKP_{0}] ERROR: Lookup key '{1}' not found in incoming row.".format(self.name, val))
								# fallback to 'val'?
								column_compare_with == self.LKP_VAL_VAL
								
						elif column_compare_with == self.LKP_VAL_EXP:
							compare_val = val	#?????? evaluate this somehow ?????
						elif column_compare_with == self.LKP_VAL_REX:
							compare_val = val	#?????? evaluate this somehow ?????
						else:
							raise UnrecoverableError("[LKP_{0}] ERROR: Invalid value ('{1}') for _compare_with_ key, please use the constants.".format(self.name, val))
						
						bind_name = 'param_{}'.format(cond_i)
						#binds[bind_name] = val		#VAL
						#COL:
						if row is not None and val in row:
							binds[bind_name] = str(row[val]).lower()	#COL	#lower to match func.lower
						else:
							binds[bind_name] = None
						cond_i+=1
		return binds

	def _create_pandas_query(self, comparison, row=None):
		"""Create pandas query string for given comparison.
		
		Args:
			comparison  (see class description)
			row 		(dict) optional row data, in case LKP_VAL_COL used.
			
		Returns:
			Pandas query string (str)
		"""
		pandas_query_and = []
		for group_and in comparison:
			pandas_query_or = []
			for condition in group_and:			
			
				# defaults
				column_operator = "=="
				column_compare_with = self.LKP_VAL_COL
				
				# get comparison settings
				for key, val in condition.items():
					if key == '_operator_':
						column_operator = self._get_oper_pandas(oper=val)

					if key == '_compare_with_':
						if isinstance(val, int):
							column_compare_with = val
						else:
							raise UnrecoverableError("[LKP_{0}] ERROR: Key '_compare_with_' must be int datatype, please use the constants.".format(self.name))
			
				# process conditions (sort by key to match bound params later)
				for key, val in sorted(condition.items()):
					if key in ('_operator_','_compare_with_') :
						pass
					elif val is not None:
						if column_compare_with == self.LKP_VAL_COL:
							if not val in row:
								raise UnrecoverableError("[LKP_{0}] ERROR: Key '{1}' not found row.".format(self.name, val))
							if row[val] is not None:
								compare_val = "'" + str(row[val]).lower() + "'"
							else:
								compare_val = None
						elif column_compare_with == self.LKP_VAL_VAL:
							compare_val = "'"+val+"'"
						elif column_compare_with == self.LKP_VAL_EXP:
							compare_val = val	#?????? evaluate this somehow ?????
						elif column_compare_with == self.LKP_VAL_REX:
							compare_val = val	#?????? evaluate this somehow ?????
						else:
							raise UnrecoverableError("[LKP_{0}] ERROR: Invalid value ('{1}') for _compare_with_ key, please use the constants.".format(self.name, val))
							
						#ToDo: (simple ascii) case insensitive search
						pandas_query_or.append("{0}{1}{2}".format(column(key), column_operator, compare_val))
						#pandas_query_or = pandas_query_or + ' | '.join(["{0}{1}{2}".format(column(key), column_operator, compare_val)])
						
					else:
						#not sure how to do an 'is Null' in pandas...
						pandas_query_or.append("{0}{1}{2}".format(column(key), column_operator, compare_val))
						
			pandas_query_and.append(' | '.join( pandas_query_or ))
		return ' & '.join(pandas_query_and)
		
		
	@ContextProcessor
	def connect(self, context, *, engine):
		"""Connect to database.
		
		This is a ContextProcessor, it will be executed once at construction of
		the class. All @ContextProcessor functions will get executed in the
		order in which they are defined.
		
		Upon successful connection it will pass the connection
		object to the next ContextProcessor.
		
		Args:
			engine (service)
			
		Returns:
			connection
		"""
		try:
			connection = engine.connect()
		except OperationalError as exc:
			raise UnrecoverableError("[LKP_{0}] ERROR: Could not create SQLAlchemy connection: {1}.".format(self.name, str(exc).replace('\n', ''))) from exc
	
		with connection:
			yield connection
			
	@ContextProcessor
	def dataset(self, context, connection, *, engine):
		"""Create the source-data dataset.
		
		Class instance variables:
		- self.select		(sqlalchemy)   Select object for source data selection
		- self.cols_return	(dict)		   Return key aliases {return_alias:source_selection_key}
		- self.cols_lookup	(list of str)  Plain text columns used in comparison
		- self.cols_source	(list of str)  Plain text source data columns
		- self.row_count_source  (int)     Number of rows in source-data
							
		"""
		self.select = None
		self.cols_return = {}
		self.cols_lookup = []
		self.cols_source = []
		self.row_count_source = None
		
		source_query_cols = []	#list of SQLAlchemy Columns
		source_query_binds = {}
		meta = MetaData()
		
		# check option combinations validity
		if (self.caching == self.LKP_CACHING_DISABLED
		   and (self.sql_override is not None or self.order_by is not None)):
			raise UnrecoverableError("[LKP_{}] ERROR: 'caching': Caching disabled, but a 'sql_override' query and/or 'order_by' provided.".format(self.name))

		if self.caching == self.LKP_CACHING_SQLITE_MEM and self.sql_override is not None and self.sql_override_cols is None:
			logger.warning("[LKP_{}] 'caching': SQLite memory table requested, but this is impossible for a text-based SQL override unless a 'sql_override_cols' is specifed. Using pandas.".format(self.name))
			self.caching = self.LKP_CACHING_PANDAS
			
		if (self.multiple_match == self.LKP_MM_FST or self.multiple_match == self.LKP_MM_LST) and self.order_by is None:
			logger.warning("[LKP_{}] 'multiple_match': Use First/Last specified, but no 'order_by' provided.".format(self.name))
			
		self.cols_lookup = self._get_cols_from_comparison(self.comparison)
				
		"""
		Handle non-overriden SQL.
		"""
		if self.sql_override is None:
		
			# check option validity
			if self.table_name is None:
				raise UnrecoverableError("[LKP_{0}] ERROR: No table_name and no SQL-override specified (specify at least one).".format(self.name))
		
			if not engine.dialect.has_table(engine, self.table_name):
				raise UnrecoverableError("[LKP_{0}] ERROR: Lookup table '{1}' does not exist.".format(self.name, self.table_name))

			meta.reflect(bind=engine, views=True, only=[self.table_name])
			
			if self.ret_fields is None:
				self.ret_fields = [col_name.name for col_name in meta.tables[self.table_name].c]
			self.cols_return = self._get_return_columns(self.ret_fields)

			# cols_source is a unique list of all columns in the source data
			self.cols_source = [source for alias, source in self.cols_return.items()]
			self.cols_source.extend(self.cols_lookup)
			self.cols_source = list(set(self.cols_source))
			
			# THIS GIVES WARNINGS:
			#WARN|0004|py.warnings: C:\...\site-packages\sqlalchemy\sql\elements.py:4390: SAWarning: Textual column expression '...' should be explicitly declared with text('...'), or use column('...') for more specificity
			for col_name in self.cols_source:
				#remove .name to get the fully qualified column name
				source_query_cols.append( Column( name=meta.tables[self.table_name].c[col_name].name
												, type_=meta.tables[self.table_name].c[col_name].type ))
			
			# source data selection
			self.select = select(source_query_cols).select_from(meta.tables[self.table_name])
						
			# order by
			if self.order_by is not None:
				order_by_cols = self._get_orderby_cols(self.order_by)
				if order_by_cols:
					self.select = self.select.order_by(*order_by_cols)
					
		"""
		Handle SQL-override.
		
		The list of return fields, when empty, cannot be derived using reflect-
		ion, instead we can derive it by executing the query and take the list
		of returned columns.
		"""
		if self.sql_override is not None:
		
			order_by_cols = None
		
			# check option validity
			if self.table_name is not None:
				logger.warning("[LKP_{0}] Ignoring table_name option because sql_override specified.".format(self.name))
			
			# order by
			if self.order_by is not None:
				order_by_cols = self._get_orderby_cols(self.order_by)
				if order_by_cols is not None:
					self.sql_override = self.sql_override + " ORDER BY " + ", ".join([str(ob) for ob in order_by_cols])
			
			# convert string to an sqlalchemy Textual selection
			if isinstance(self.sql_override, str):
				self.select = text(self.sql_override)

			#if not isinstance(self.select, sqlalchemy.sql.Select):
			#	raise UnrecoverableError("[LKP_{0}] ERROR: Could not create SQLAlchemy select statement.")
		
		"""
		Determine caching strategy.
		"""
		if self.caching == self.LKP_CACHING_ENABLED:
			if self.sql_override is not None and isinstance(self.sql_override, str):
				logger.info("[LKP_{0}] Using pandas for source data caching.".format(self.name))
				self.caching = self.LKP_CACHING_PANDAS
			else:
				logger.info("[LKP_{0}] Using SQLite memory table for source data caching.".format(self.name))
				self.caching = self.LKP_CACHING_SQLITE_MEM
				
		elif self.caching == self.LKP_CACHING_DISABLED:
			logger.info("[LKP_{0}] Caching disabled.".format(self.name))

		"""
		Create source dataset.
		"""
		if self.caching == self.LKP_CACHING_PANDAS:
			dataset = pandas.read_sql_query(self.select, connection, params=source_query_binds)
			
			if dataset is None:
				logger.warning("[LKP_{0}] No source data found!".format(self.name))
				self.row_count_source = 0
			else:
				self.row_count_source = len(dataset)
				
				# BASED ON SOURCE DATA
				if self.ret_fields is None:
					self.ret_fields = [col_name for col_name in dataset.columns]
				self.cols_return = self._get_return_columns(self.ret_fields)
				
				# creates sanitized class instance column lists
				self.cols_return = self._get_return_columns(self.ret_fields)
				
				# cols_source is a unique list of all columns in the source data
				self.cols_source = [source for alias, source in self.cols_return.items()]
				self.cols_source.extend(self.cols_lookup)
				self.cols_source = list(set(self.cols_source))
						
		elif self.caching == self.LKP_CACHING_SQLITE_MEM:
		
			source_col_datatype = []
		
			dataset = connection.execute(self.select, source_query_binds).fetchall()

			if dataset is None:
				logger.warning("[LKP_{0}] No source data found!".format(self.name))
				self.row_count_source = 0
			else:
				self.row_count_source = len(dataset)
			
			if source_query_cols:					
				source_col_datatype = copy.deepcopy(source_query_cols)	# Metadata may get changed when used in different meta context
			else:
			
				if self.sql_override_cols is not None:
					print("debug-B")
					source_col_datatype = self.sql_override_cols
				else:
					for i_col, col_name in enumerate(self.cols_source):
						col_datatype = self._get_sqlite_datatype(python_type=type(dataset[0][i_col]))
						if col_datatype is None:
							if self.row_count_source > 10:
								scan_limit = 9
							else:
								scan_limit = self.row_count_source-1
							for i in range(0,scan_limit):
								col_datatype = self._get_sqlite_datatype(python_type=type(dataset[i][i_col]))
								if col_datatype is not None:
									break
									
						if col_datatype is None:
							logger.warning("[LKP_{0}] Scanned first {1} rows; could not determine data type for column '{2}', assuming TEXT.".format(self.name, scan_limit+1, col_name))
							col_datatype = String()
						print("debug-C")
						source_col_datatype.append( Column(col_name, col_datatype) )

			# setup memory table
			self.mem_engine = create_engine('sqlite:///:memory:', echo=False)
			self.mem_connection = self.mem_engine.connect()		
			source_cols_sd = copy.deepcopy(source_col_datatype)		# still required? if we don't copy the underlying metadata binds get changed
			meta_sd = MetaData()
			
			print(source_query_cols)
			print("-------------")
			
			source_data_tbl = Table('source_data', meta_sd , *source_cols_sd )
			#source_data_tbl = Table('source_data', meta_sd , *source_col_datatype )
			meta_sd.create_all(self.mem_engine)	
			meta_sd.reflect(bind=self.mem_engine)
			
			# insert into mem-database
			ins_source_data = meta_sd.tables['source_data'].insert()
			for row in dataset:
				self.mem_connection.execute(ins_source_data.values().values(row))		
		
			# BASED ON SOURCE DATA
			if self.ret_fields is None:
				self.ret_fields = [col_name.name for col_name in meta_sd.tables['source_data'].c]
			self.cols_return = self._get_return_columns(self.ret_fields)
			
			# creates sanitized class instance column lists
			self.cols_return = self._get_return_columns(self.ret_fields)
			
			# cols_source is a unique list of all columns in the source data
			self.cols_source = [source for alias, source in self.cols_return.items()]
			self.cols_source.extend(self.cols_lookup)
			self.cols_source = list(set(self.cols_source))

		elif self.caching == self.LKP_CACHING_DISABLED:
			dataset = {}
		
		print("-- [ LKP_{} ] ------------------------------------".format(self.name))
		print("Selection query: {}".format(str(self.select)))
		print("Bind values    : {}".format(source_query_binds))
		print("Row Count      : {}".format(self.row_count_source))
		print("Multiple Match : {}".format(self.multiple_match))
		print("Caching        : {}".format(self.caching))
		print("--------------------------------------------------")
		
		yield dataset
		
	
	def __call__(self, connection, dataset, context, d_row_in, engine):
		"""Row processor."""
		
		d_row_out = {**d_row_in}
		
		lkp_key_special = "__LKP_{}__".format(self.name)		
		if self.name != 'untitled':
			d_row_out[lkp_key_special] = {
				  'CACHING'        : self.caching
				, 'MULTIPLE_MATCH' : self.multiple_match
				, 'ROWS_SOURCE'    : self.row_count_source
			}
		
		# get lookup result
		if self.caching == self.LKP_CACHING_DISABLED:
			sql_select = self._append_where_clause(self.select, self.comparison)
			sql_bound = self._get_sql_binds(self.comparison, d_row_out)
			lkp_result = connection.execute(sql_select, sql_bound).fetchall()			
				
		elif self.caching == self.LKP_CACHING_SQLITE_MEM:
			lkp_result = self._lkp_sqlitemem(row=d_row_out)
			
		elif self.caching == self.LKP_CACHING_PANDAS:
			lkp_result = self._lkp_pandas(dataset=dataset, row=d_row_out)
		
		yield from lkp_result
		
		"""
		# process lookup result
		lkp_rows_found = len(lkp_result)
		
		if lkp_rows_found == 1:
			#return lkp_result	# <-- does not work, must be an iterator???? lkp_result is a list
			yield from lkp_result
			
		elif lkp_rows_found > 1 and self.multiple_match == self.LKP_MM_ALL:
			yield from lkp_result
				
		elif lkp_rows_found > 1 and self.multiple_match == self.LKP_MM_LOV:
			return lkp_result
		"""
		
	def _lkp_sqlitemem(self, row):
		"""Lookup on a SQLite memory table.
				
		Args:
			row	(dict)
			
		Returns:
			row (list of dict)
		"""
		lkp_key_special = "__LKP_{}__".format(self.name)
		
		# init stuff... move to context processor?
		meta = MetaData()
		meta.reflect(bind=self.mem_engine)
		source_data_select = select(self.cols_source).select_from(meta.tables['source_data'])
		sql_select = self._append_where_clause(source_data_select, self.comparison)
		
		# execute comparison query
		sql_bound = self._get_sql_binds(self.comparison, row)
		result = self.mem_connection.execute(sql_select, sql_bound)
				
		# create return dataset (always return a list!)
		if self.multiple_match == self.LKP_MM_FST:
			ret_dataset = result.fetchfirst()
		elif self.multiple_match == self.LKP_MM_LST:
			logger.info("[LKP_{0}] 'multiple_match': It's more efficient to choose LKP_MM_FST and reverse your order by.".format(self.name))
			ret_dataset_all = result.fetchall()
			ret_dataset = ret_dataset_all[len(ret_dataset_all)-1]
		elif self.multiple_match == self.LKP_MM_ANY:
			ret_dataset = result.fetchone()
		else:
			#LKP_MM_LOV, LKP_MM_ALL
			ret_dataset = result.fetchall()
		
		# not available for SQLite-mem
		# could be implemented quite simply, but is this number really worth the performance impact?
		if lkp_key_special in row:
			row[lkp_key_special]['ROWS_MATCHED'] = -1
		
		return self._get_result_as_list(ret_dataset, row)
		
	def _lkp_pandas(self, dataset, row):
		"""Lookup on a pandas DataFrame.
		
		Args:
			dataset	(pandas dataframe)
			row	(dict)
			
		Returns:
			row (list of dict)
		"""
		lkp_key_special = "__LKP_{}__".format(self.name)
		pquery = self._create_pandas_query(comparison=self.comparison, row=row)
		result = dataset.query(pquery)
		
		# limit resultset based on multiple_match setting
		if len(result) == 0:
			ret_dataset = None
		else:
			if self.multiple_match == self.LKP_MM_FST or self.multiple_match == self.LKP_MM_ANY:
				ret_dataset = [result.iloc[0]]
			elif self.multiple_match == self.LKP_MM_LST:
				ret_dataset = [result.iloc[len_result-1]]
			else:
				#LKP_MM_LOV, LKP_MM_ALL
				ret_dataset = result
		
		return self._get_result_as_list(ret_dataset, row)
		
	def _get_result_as_list(self, ret_dataset, row):
		"""Merge row and resultset and return as list.
		
		May return multiple rows .
		
		Returns the incoming row, appended with the 'lookup columns'.
		May return one or multiple rows in case LKP_MM_ALL specifed,
		but always a list.
		
		Args:
			dataset (must be addressable by name)
			row	    (dict)

		Returns:
			row (list of dict)
		"""
		# inspect the number of results
		if ret_dataset is None:
			len_ret_dataset = 0
		elif not isinstance(ret_dataset, list):
			ret_dataset = [ret_dataset]
			len_ret_dataset = 1
		else:
			len_ret_dataset = len(ret_dataset)
	
		# lookup returned no rows
		# return incoming row as list, appended with empty lookup columns
		if len_ret_dataset == 0:
		
			# loop thru return cols
			for ret_alias in self.cols_return.keys():
				key_out = '.'.join(filter(None, [self.ret_prefix,ret_alias]))
				row[key_out] = None
			return [row]
		
		# lookup returned one row
		# return incoming row as list, appended with lookup columns
		elif len_ret_dataset == 1:
			# loop thru return cols
			for ret_alias, source_col in self.cols_return.items():
				key_out = '.'.join(filter(None, [self.ret_prefix,ret_alias]))
				if self.multiple_match == self.LKP_MM_LOV:
					row[key_out] = [ret_dataset[0][source_col]]
				else:
					row[key_out] = ret_dataset[0][source_col]
			return [row]
		
		# lookup returned more than one row
		# return list of row appended with returned rows
		else:
			if self.multiple_match == self.LKP_MM_ERR:
				raise UnrecoverableError("[LKP_{0}] ERROR: Lookup returned more than one row and LKP_MM_ERR specified.".format(self.name))
						
			else: # self.multiple_match == self.LKP_MM_ALL: / self.LKP_MM_LOV
			
				l_lookup_rows = []
				# loop thru returned rows
				for row_id, ret_row in enumerate(ret_dataset):
									
					# loop thru return cols
					for ret_alias, source_col in self.cols_return.items():
						key_out = '.'.join(filter(None, [self.ret_prefix,ret_alias]))
						if self.multiple_match == self.LKP_MM_LOV:
							row[key_out] = [ret_row[source_col]]
						else:
							row[key_out] = ret_row[source_col]
					
					if lkp_key_special in row:
						row[lkp_key_special]['ROW_NR'] = row_id+1
						
					l_lookup_rows.append(row)

				return l_lookup_rows

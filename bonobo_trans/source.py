from bonobo.config import Configurable, ContextProcessor, Option, Service
from bonobo.config import use_context
import sqlalchemy

@use_context
class DbSource(Configurable):
	"""The DbSource transformation extracts data from a SQL database.

	.. note::
		Database connectivity is provided by SQLAlchemy. The engine must be
		provided via a *Bonobo Service*, containing an 'sqlalchemy.engine'.

	.. tip::
		From a Bonobo point of view this transformation is not a true
		"extractor", the tranformation allows (but does not require) to pass
		one(!) row to it. The keys of this rows will be appended to all
		outgoing rows.

	.. admonition:: Configuration options
		
		**Required:**
		
			-	**table_name** *(str)*

		*Optional:*
		
			-	**sql_select**		*(str or sqlalchemy.sql)*
			-	**sql_pre**			*(str or sqlalchemy.sql)*
			-	**sql_filter**		*(str or sqlalchemy.where)*
			-	**bp_select**		*(dict)*
			-	**bp_pre_sql**		*(dict)*
			-	**distinct**		*(bool)*
			-	**ordered_cols**	*(int or list of str)*
			-	**verbose_sql**		*(bool)*, default: False
			-	**verbose_data**	*(bool)*, default: False
			-	**streaming**		*(bool)*, default: False
			-	**row_counters**	*(bool)*, default: True
			-	**keep_alive**		*(bool)*, default: False			

	**Description of the selection options:**

		sql_select, sql_pre
			Either provide a 'raw' SQL statement or a SQLAlchemy sql object.
			All columns and records will be selected if no selection is provided.

		sql_filter
			Source filters can be specified in the ``sql_select`` parameter and/or in
			the ``sql_filter`` parameter. (Additional) filters specified in the
			sql_filter will be appended to the where clause (AND?).
			
			.. important::
				It is not possible to specify an ``sql_filter`` when the ``sql_select`` contains
				an ORDER BY clause!
				
		distinct
			When True, applies the SQL *distinct* to source query. Not
			available if the ``sql_select`` is a plain string.
		
		ordered_cols
			An ORDER BY should ideally be specified via the ``ordered_cols`` option
			rather than via an "ORDER BY"-clause in the ``sql_select`` option.
			The ``ordered_cols`` parameter can take any of the following formats:
			
			* a list of columns (str)
			* a list of column positions (int)
			* an integer specifying how many columns are sorted, eg. "3", would
			  order by the first three columns.
			
			.. important::
				When using the ordered_cols, you cannot already have an order by clause
				in the sql_select.

		bp_select, bp_pre_sql
			Bind parameters for the selection and pre-sql statements.
	
			.. attention::
				Bind parameters must be provided using a dictionary. The format for
				this is: ``{'parameter': 'value'}``
				
				The name of the parameter depends on the your wether you're using a
				'raw' SQL statement or an SQLAlchemy.sql object.
				
				Raw SQL:
					For 'raw' SQL, bound parameters are specified by name. For example::
					
					  sql_select = 'SELECT * FROM users WHERE id=:user_id'
					  bp_select = { 'user_id':555 }

				SQLAlchemy:
					For an sqlalchemy.sql object, bound parameters can be specified in
					various different manners. Make sure the key in the dictionary matches.
					For example::
					
					  sql_select = select([users]).where(users.c.id == bindparam('user_id'))
					  bp_select = { 'user_id':555 }
				
		name
			Name of the transformation, for identification and logging purposes.
			
		verbose_sql
			If 'verbose_sql' is True, the generated SQL statement will be printed.

		verbose_data
			If 'verbose_data' is True, every outgoing row is printed.

		streaming
			If 'streaming' is False, the query is executed and fetched. Only after it
			is received completely will it start 'yielding' the data. This enables
			use of the __ROW_NR_TOTAL__ field. If this setting is True, a total
			row count will not be available. Set to True for large datasets. Set to
			False to use the total row count, or to avoid reading and writing the
			same database table.

		row_counters
			If 'row_counters' is True, the additional keys __ROW_NR__ and
			__ROW_NR_TOTAL__ will be added to the outgoing row.

		keep_alive
			If 'keep_alive' is	True it will keep the connection open. This might
			block access on single user databases like SQLite.

	Args:
		*	**d_row_in** *(dict)*

	Returns:
		*	**d_row_out** *(dict)*

		d_row_out is a dictionary containing all the keys of the incoming
		dictionary plus keys (columns) of the selected data. Any keys in the
		incoming record that are identical to selected columns are overwritten.

		Additionally the following special keys may be added:

		-	__ROW_NR__				(int)  Row counter.
		-	__ROW_NR_TOTAL__		(int)  Total number of selected rows.
		-	__ORDERED__				(bool) Data is sorted (not implemented yet).	
	"""

	engine = Service('sqlalchemy.engine')
	table_name   = Option(required=True, type=str)
	sql_select   = Option(required=False)
	sql_pre	     = Option(required=False)
	sql_filter   = Option(required=False)
	bp_select    = Option(required=False, type=dict, default={})
	bp_pre_sql   = Option(required=False, type=dict, default={})
	distinct     = Option(required=False, type=bool, default=False)
	ordered_cols = Option(required=False)
	name         = Option(required=False, type=str,  default="untitled")
	verbose_sql  = Option(required=False, type=bool, default=False)
	verbose_data = Option(required=False, type=bool, default=False)
	streaming    = Option(required=False, type=bool, default=False)
	row_counters = Option(required=False, type=bool, default=True)
	keep_alive   = Option(required=False, type=bool, default=False)

	@ContextProcessor
	def setup(self, context, *, engine):
		"""This context processor initializes the tranformation.

		If keep_alive is False, the connection will be closed, otherwise it
		will be yielded.

		Args:
			engine
		"""
		
		def exec_pre_sql(connection):
			pre_sql_kwargs = {}
			if self.bp_pre_sql is not None:
				for key, val in self.bp_pre_sql.items():
					pre_sql_kwargs[key] = val
			connection.execute(self.sql_pre, pre_sql_kwargs)

		def is_valid_sql(sql):		
			#if (     not isinstance(sql, str)
			#	 and not isinstance(sql, sqlalchemy.sql.selectable.Select) ):
			if not isinstance(sql, (str, sqlalchemy.sql.selectable.Select,)) :
				return False
			else:
				return True

		try:
			connection = engine.connect()
		except OperationalError as exc:
			raise UnrecoverableError("[SRC_{0}] Could not create SQLAlchemy connection: {1}.".format(self.name, str(exc).replace('\n', ''))) from exc

		if not engine.dialect.has_table(engine, self.table_name):
			#raise error
			print("[SRC_{0}] ERROR: Table '{1}' does not exist.".format(self.name, self.table_name))
				
		meta = sqlalchemy.MetaData()
		meta.reflect(bind=engine)
			
		self.attributes = [attribute.name for attribute in meta.tables[self.table_name].c]
		self.rows_selected = 0

		if self.sql_select is None:
			self.sql_select = meta.tables[self.table_name].select()

		if not is_valid_sql(self.sql_select):
			# raise error
			pass

		if self.sql_pre is None:
			pass
		elif is_valid_sql(self.sql_pre):
			self.exec_pre_sql(connection)
		else:
			# raise error
			pass

		# todo implement:
		# sql_filter
		# ordered_cols

		if self.distinct and isinstance(self.sql_select, str):
			raise UnrecoverableError("[SRC_{0}] Not possible to request 'distinct' on a textual SQL query.".format(self.name))
		elif self.distinct:
			self.sql_select = self.sql_select.distinct()
		
		if self.keep_alive:
			with connection:
				yield connection
		else:
			connection.close()
			yield connection


	def __call__(self, connection, context, d_row_in=None, *, engine):
	#def __call__(self, connection, context, d_row_in, engine):

		if self.verbose_sql:
			print("[SRC_{0}] Source select: {1}; Bind parameters: {2}".format(self.name, self.sql_select, self.bp_select))

		if not self.keep_alive:
			connection = engine.connect()

		# verbose_data
		# streaming
		# row_counters
		# keep_alive

		# STREAMING

		#with connection:
		#		 result = conn.execute(self.sql_select,self.bp_select)
		#		 yield from result.


		# ORIGINAL:

		with connection:
			result = connection.execute(self.sql_select,self.bp_select)
			source_rows = result.fetchall() # list of RowProxy
			row_count_source = len(source_rows)

			if row_count_source == 0:
				print(" > No source rows. Starting next phase.")
				print("")
				#etl_phase += 1
				# TODO?? HOW?
			else:
				print(" > Processing {0} row(s)".format(row_count_source))

				# TODO: CONVERT TO NAMEDTUPLE
				# NOW: CONVERTING TO DICT
				l_source_rows = []
				for row_id, row in enumerate(source_rows):
					
					if isinstance(d_row_in, dict):
						d_source_data = {**d_row_in}
					else:
						d_source_data = {}

					for key, val in row.items():
							d_source_data[key] = val

					# ADD SOME ROW METADATA; ROWNUMBER:
					d_source_data['ROW_NR'] = row_id+1										# old style
					d_source_data['ROW_NR_TOTAL'] = row_count_source		# old style
					d_source_data['__ROW_NR__'] = row_id+1
					d_source_data['__ROW_NR_TOTAL__'] = row_count_source
					l_source_rows.append(d_source_data)

			yield from l_source_rows
					

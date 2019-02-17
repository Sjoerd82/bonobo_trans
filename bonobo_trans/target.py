from bonobo.config import Configurable, ContextProcessor, Option, Service
from bonobo.config import use_context
import sqlalchemy

@use_context
class DbTarget(Configurable):
	"""The DbTarget transformation writes data to a SQL database.

	.. note::
		Database connectivity is provided by SQLAlchemy. The engine must be
		provided via a *Bonobo Service*, containing an 'sqlalchemy.engine'.

	.. tip::
		From a Bonobo point of view this transformation is not a true
		"loader", the tranformation allows (but does not require)
		transformations after it to keep processing the data.

	.. admonition:: Configuration options
	
		**Required:**
		
			-	**table_name** *(str)*

		*Optional:*
		
			-	**operation**		 *(int)*
			-	**upsert_strategy**	 *(int)*
			-	**sql_insert**		 *(str or sqlalchemy.sql.insert)*	Override
			-	**sql_update**		 *(str or sqlalchemy.sql.update)*	Override
			-	**sql_delete**		 *(str or sqlalchemy.sql.delete)*	Override
			-	**sql_pre**			 *(str or sqlalchemy.sql)*			Pre-SQL
			-	**bp_update**		 *(dict)*
			-	**bp_pre_sql**		 *(dict)*
			-	**target_key**		 *(str or list of str)*			Override?
			-	**verbose_sql**		 *(bool)*			False
			-	**verbose_data** 	 *(bool)*			False
			-	**truncate**		 *(bool)*			False
			-	**dry_run**			 *(bool)*			False
			-	**simulate**		 *(bool)*			False
	
	**Description of the options:**

		operation, upsert_strategy
			``operation`` can hold any of the following constants:
		
			===================== =============================================
			``operation``         Description
			--------------------- ---------------------------------------------
			TGT_OPER_DATA_DRIVEN  Operation is determined by '__TARGET_OPERATION__' field.
			TGT_OPER_INSERT_ONLY  Only insert into target table (default?)
			TGT_OPER_UPDATE_ONLY  Only update target table, based on target_key ? or an actual key ???????? TBD
			TGT_OPER_UPSERT       Update key in target, otherwise insert if key doesn't exist yet.
			TGT_OPER_DELETE       Deletes by PK definition or by provided key. Target table must have a primary key, if no delete key specified.
			TGT_OPER_REJECT       Not implemented
			===================== =============================================
			
			If ``operation`` is TGT_OPER_UPSERT, the ``upsert_strategy`` option
			can be used to choose one of the following options, specifiying how
			to execute the upsert:
			
			============================== =============================================
			``upsert_strategy``             Description
			------------------------------ ---------------------------------------------
			TGT_UPSERT_TRY_INSERT			Try to insert, if fails try to update
			TGT_UPSERT_EXISTS				If exist update, else insert
			TGT_UPSERT_EXISTS_SERIALIZABLE	Same, but with: WITH(UPDLOCK) and SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
			TGT_UPSERT_MERGE				Upsert using merge statement
			TGT_UPSERT_MERGE_SERIALIZABLE	Same, but with: SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
			============================== =============================================
	
		sql_insert, sql_update, sql_delete	
			Either provide a 'raw' SQL statement or a SQLAlchemy sql object.
	
			The target SQL is normally generated based on the operation parameter.
			It is possible to provide a custom SQL statement via one of the three
			override parameters.
			
			The provided override must still be in accordance with the operation in
			order to be executed. Would you, for example, provide a sql_insert
			override when the operation is TGT_OPER_DELETE, the sql_insert override
			would be ignored.
		
		bp_select, bp_pre_sql
			Bind parameters for the selection and pre-sql statements.
	
			.. attention::
				Bind parameters must be provided using a dictionary. The format for
				this is: ``{'parameter': 'value'}``
				
				The name of the parameter depends on the your wether you're using a
				'raw' SQL statement or an SQLAlchemy.sql object.
				
				Raw SQL:
					For 'raw' SQL, bound parameters are specified by name. For example: 
					``sql_pre = 'DELETE * FROM users WHERE id=:user_id'``
					``bp_select = { 'user_id':555 }``

				SQLAlchemy:
					For an sqlalchemy.sql object, bound parameters can be specified in
					various different manners. Make sure the key in the dictionary matches.
					For example:
					``sql_pre = delete().where(users.c.id == bindparam('user_id'))``
					``bp_select = { 'user_id':555 }``

		verbose_sql
			If 'verbose_sql' is True, the generated SQL statement will be printed.

		verbose_data
			If 'verbose_data' is True, every outgoing row is printed.
			
		truncate
			Truncate target table before writing.
			
		dry_run
			Don't actually write to target. Useful in combination with
			verbose_sql and/or verbose_data.
	
		simulate
			Not implemented. Possible future feature: copy target table into
			memory table to simulate a target load
			
	Args:
		*	**d_row_in** *(dict)*

		d_row_in is a dictionary containing key for some or all of the target
		columns.
		
		Additionally the following special keys may be added:

		-	__TARGET_TABLE__		(str)	Used in conjuction with "Data Driven" operation.
		-	__TARGET_OPERATION__	(str)	Override specified operation.
		
	Returns:
		*	**d_row_out** *(dict)*
		
	"""

	TGT_OPER_DATA_DRIVEN = 0
	TGT_OPER_INSERT_ONLY = 1
	TGT_OPER_UPDATE_ONLY = 2
	TGT_OPER_UPSERT = 3
	TGT_OPER_DELETE = 4
	TGT_OPER_REJECT = 5

	TGT_UPSERT_TRY_INSERT = 0
	TGT_UPSERT_EXISTS = 1
	TGT_UPSERT_EXISTS_SERIALIZABLE = 2
	TGT_UPSERT_MERGE = 3
	TGT_UPSERT_MERGE_SERIALIZABLE = 4

	engine = Service('sqlalchemy.engine')
	table_name	 = Option(required=True,  type=str)
	operation    = Option(required=False, type=int, default=TGT_OPER_INSERT_ONLY)
	sql_insert   = Option(required=False)
	sql_update   = Option(required=False)
	sql_delete   = Option(required=False)
	sql_pre		 = Option(required=False)
	bp_update	 = Option(required=False, type=dict)
	bp_pre_sql	 = Option(required=False, type=dict)
	name         = Option(required=False, type=str,  default="untitled")
	verbose_sql	 = Option(required=False, type=bool, default=False)
	verbose_data = Option(required=False, type=bool, default=False)
	truncate     = Option(required=False, type=bool, default=False)
	dry_run      = Option(required=False, type=bool, default=False)
	simulate     = Option(required=False, type=bool, default=False)
	
	@ContextProcessor
	def setup(self, context, *, engine):
		"""This context processor initializes the tranformation.
		
		Args:
			engine
		"""
		
		try:
			connection = engine.connect()
		except OperationalError as exc:
			raise UnrecoverableError("[TGT_{0}] Could not create SQLAlchemy connection: {1}.".format(self.name, str(exc).replace('\n', ''))) from exc
			
		if not engine.dialect.has_table(engine, self.table_name):
			#raise error
			print("[TGT_{0}] ERROR: Table '{1}' does not exist.".format(self.name, self.table_name))

		self.rows_inserted = 0
		self.rows_updated = 0
		self.rows_upserted = 0
		self.table = None
		self.attributes = None
		self.run_once_executed = False

		meta = sqlalchemy.MetaData()
		meta.reflect(bind=engine)
		
		#self.table = meta.tables['sys_batch']
		#self.table = Table('sys_batch', meta)		
		#self.attributes = [attribute.name for attribute in self.table.c]
		#self.sql_insert = self.table.insert()
		#self.sql_update = self.table.update()
		#self.sql_delete = self.table.delete()
		
		self.attributes = [attribute.name for attribute in meta.tables[self.table_name].c]
		
		if self.sql_insert is None:
			self.sql_insert = meta.tables[self.table_name].insert()
			
		if self.sql_update is None:
			self.sql_update = meta.tables[self.table_name].update()
			
		if self.sql_delete is None:
			self.sql_delete = meta.tables[self.table_name].delete()

		if self.sql_pre is not None:
			pre_sql_kwargs = {}
			# Prepare bind parameters
			if self.bp_pre_sql is not None:
				for key, val in self.bp_pre_sql.items():
					pre_sql_kwargs[key] = val #d_row[val]
			
			connection = engine.connect()
			with connection:
				if not self.dry_run:
					connection.execute(self.sql_pre,pre_sql_kwargs)
					self.run_once_executed = True
				if self.verbose_sql:
					print("Pre-SQL: {0}; Binds: {1}".format(self.sql_pre,pre_sql_kwargs))
					
		with connection:
			yield connection
	
	def __call__(self, connection, context, d_row, engine):
		""" d_row is a dict """

		d_target = {**d_row}	#otherwise we delete keys from the still in parallely used options transformation!!!!!
		
		# Check row data
		if '__TARGET_TABLE__' in d_target:
			if not d_target['__TARGET_TABLE__'].upper() == self.table_name.upper():
				# this record is not meant for this target
				return d_row # return original record (untouched)
			
		if '__TARGET_OPERATION__' in d_target:
			pass
		
		# Prepare bind parameters
		upd_kwargs = {}
		if self.bp_update is not None:
			for key, val in self.bp_update.items():
				if val in d_target:
					upd_kwargs[key] = d_target[val]
				else:
					# raise error?
					pass
	
		# Remove any key from d_target dict that does not occur in the actual target table
		list_of_keys = []
		for key in d_target:
			list_of_keys.append(key)
		
		for key in list_of_keys:
			if key not in self.attributes:
				del d_target[key]
							
		# Prepare SQL insert and update statements
	
		# INSERT
		if self.sql_insert is not None:
			local_sql_insert = self.sql_insert.values(d_target)
		
		# UPDATE
		if self.sql_update is not None:
			local_sql_update = self.sql_update.values(d_target)
		
		# todo: check upsert strategy
		# TGT_UPSERT_TRY_INSERT
		connection = engine.connect()
		with connection:
			try:
				if not self.dry_run:
					connection.execute(local_sql_insert)
					
				if self.verbose_sql and self.rows_inserted == 0:
					print(local_sql_insert)
					
				self.rows_inserted += 1
				
			except sqlalchemy.exc.IntegrityError as e:
				if self.operation not in (self.TGT_OPER_INSERT_ONLY,self.TGT_OPER_DELETE,self.TGT_OPER_REJECT):
					#if upd is not None:
					if self.dry_run is False:
						connection.execute(self.sql_update,upd_kwargs)
						
					if self.verbose_sql and self.rows_updated == 0:
						print(self.sql_update)
						
					self.rows_updated += 1
					self.rows_upserted += 1
					#else:
					#print("WARNING: Insert failed and no update specified. {0}".format(e))

		#self.rows_inserted += 1;
		#return {**d_row, 'counter':self.rows_inserted}
		return d_row # return original record (untouched)

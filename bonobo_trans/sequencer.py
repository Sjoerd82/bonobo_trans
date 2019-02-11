import logging

from bonobo.config import Configurable, ContextProcessor, Option, Service
from bonobo.config import use_context
from bonobo.errors import UnrecoverableError
from sqlalchemy import Column, Integer, MetaData, String, Table, func, select
from sqlalchemy.engine import create_engine

from bonobo_trans.logging import logger

@use_context
class Sequencer(Configurable):
	"""The Sequencer transformation is a number generator.
	
	.. admonition:: Configuration options
	
		*Optional:*
		
			-	**name**          *(str, length max. 30)*
			-	**sequence_key**  *(str)*  Default: SEQ
			-	**initial**       *(int)*  Default: 1
			-	**increment**     *(int)*  Default: 1
			-	**max**           *(int)*
			-	**cycle**         *(bool)* Default: False
			-	**generator**     *(bool)* Default: False
			-	**generate**      *(int)*
			
			-	**source_value_col** *(str)*
			-	**source_value_tbl** *(str)*

			-	**persist_type**   *(int)*  Default: 
			-	**persist_table**  *(str)*
			-	**persist_file**   *(str)*
	
	**Option descriptions:**

		name
			Name of the transformation. Required when using persistence.
		
		sequence_key
			Name of the sequence key in the outgoing grow. Default is 'SEQ'.
		
		initial
			Starting value. Will start at 1 if not specified.
		
		increment
			Value to add in every increment. Will increment by 1 if not specified.
		
		max
			Maximum allowed value. When reached the sequencer will stop generating
			new numbers. If the 'cycle' option is True, the sequencer will restart
			at the initial value.
		
		cycle
			When set to True, the sequencer will restart at the initial value after
			reaching the max value.
		
		source_value_tbl, source_value_col
			Use to retrieve an initial value from an existing table. See notes
			below.
		
		.. note::
			**Row generation**
		
		generator, generate
			Use to generate rows instead of appending. See notes below.
		
		persist_type, persist_file, persist_table
			Persist sequence values. See notes below.
		
		
		source_value_tbl, source_value_col
			It's possible to start with an initial value based on an existing value
			in a database table. Provide the table and column name using the
			``source_value_tbl`` and ``source_value_col``-options.
		
		
		generator, generate
			Instead of appending a row with a sequence number it is possible to
			generate a set of rows instead. To do so, set the 'generator' option to
			True and the 'generate' option to the number of rows you want to
			generate.
			
			The generator mode is essentialy an "extract" transformation, and as
			such, no rows can be passed onto it.

			By default the generator mode is not enabled.
	
	
		.. note::
			**Persistence**
			
			Persistence enables the sequencer to continue the sequence after
			restarting. The current value will need to be stored in a database or
			in a file.
			
			By default persistence is not enabled.
			
			There is no mechanism to remove unused files, tables or table entries.
			You will need to clean-up these using ?? How to add utility functions to this class ??
		
		persist_type
			====================    ======================
			``persist_type``        Description
			--------------------    ----------------------
			SEQ_PERSIST_DISABLED    No persistence.
			SEQ_PERSIST_DB          Persist to a DB table.
			SEQ_PERSIST_FILE        Persist to a flatfile.
			====================    ======================
			
		persist_file
			When using SEQ_PERSIST_FILE, the ``persist_file`` option will need to
			hold the fully qualifed path and file name to which to save the
			sequence value.
		
		persist_table, persist_allow_creation
			When using SEQ_PERSIST_DB, the ``persist_table`` option will need to
			hold the table name to which to write the sequence value. If the
			table does not exist and 'persist_allow_creation' is True, the
			table will be created automatically. When creating the table in
			advance, you must include the following fields:
			-	sequence_name	string(30)
			-	sequence_nr     numeric
		
		
	Args:
		*	**d_row_in** *(dict)*
		
	Returns:
		*	**d_row_out** *(dict)*

		d_row_out contains all the keys of the incoming	dictionary plus the
		sequencer key (set using the 'sequence_key'-option). If there
		already is a key with that name it will be overwritten.
		
	"""
	
	SEQ_PERSIST_DISABLED = 0
	SEQ_PERSIST_DB       = 1
	SEQ_PERSIST_FILE     = 2
	
	engine = Service('sqlalchemy.engine')
	name             = Option(required=False, type=str, default="untitled")
	sequence_key     = Option(required=False, type=str, default='SEQ')
	source_value_tbl = Option(required=False, type=str)
	source_value_col = Option(required=False, type=str)
	initial          = Option(required=False, type=int, default=1)
	increment        = Option(required=False, type=int, default=1)
	max              = Option(required=False, type=int, default=None)
	cycle            = Option(required=False, type=bool,default=False)
	persist_type     = Option(required=False, type=int, default=SEQ_PERSIST_DISABLED)
	persist_table    = Option(required=False, type=str, default='SEQUENCES')
	persist_file     = Option(required=False, type=str)
	persist_allow_creation = Option(required=False, type=bool, default=True)
	generator        = Option(required=False, type=bool,default=False)
	generate         = Option(required=False, type=int, default=1)
	
	
	def _get_persisted_from_file(self):
		"""Read persisted value from file."""
		with open(self.persist_file, 'r') as f:
			return int(f.readline())

	def _get_persisted_from_db(self, engine, connection):
		"""Read persisted value from database table."""
		metadata = MetaData()
		if not engine.dialect.has_table(engine, self.persist_table):
			if self.persist_allow_creation:
				tbl_persist = Table(self.persist_table, metadata,
					Column('sequence_name', String(30), primary_key=True),
					Column('sequence_nr', Integer)
				)
				metadata.create_all(engine)
				sql_insert_seq = tbl_persist.insert().values([self.name, None])
				connection.execute(sql_insert_seq)
			return None

		else:
			metadata.reflect(bind=engine, only=[self.persist_table])
			tbl_persist = metadata.tables[self.persist_table]

			seq_exist = connection.execute(select([func.count()]).select_from(tbl_persist).where(tbl_persist.c.sequence_name==self.name)).scalar()			
			if seq_exist == 0:
				sql_insert_seq = tbl_persist.insert().values([self.name, None])
				connection.execute(sql_insert_seq)
				return None
			else:
				return connection.execute(select([tbl_persist.c.sequence_nr]).select_from(tbl_persist).where(tbl_persist.c.sequence_name==self.name)).scalar()
	
	def _get_sequence_from_db(self, engine, connection):
		"""Read starting value from a table."""
		if not engine.dialect.has_table(engine, self.source_value_tbl):
			raise UnrecoverableError("[SEQ_{0}] ERROR: 'source_value_tbl' Table doesn't exist.".format(self.name))
		else:
			metadata = MetaData()
			metadata.reflect(bind=engine, only=[self.source_value_tbl])
			tbl_source = metadata.tables[self.source_value_tbl]
			high_val = connection.execute(select(func.max([tbl_source.c.self.source_value_col])).select_from(tbl_source)).scalar()
			if high_val is None:
				return 0
			else:
				return int(high_val)
		
	def _set_persisted_to_file(self):
		"""Write persisted value to file."""
		with open(self.persist_file, 'w') as f:
			f.write(str(self.sequence_number))

	def _set_persisted_to_db(self, engine, connection):
		"""Write persisted value to database."""
		metadata = MetaData()
		metadata.reflect(bind=engine, only=[self.persist_table])
		tbl_persist = metadata.tables[self.persist_table]
		sql_update_seq = tbl_persist.update().where(tbl_persist.c.sequence_name==self.name).values(sequence_nr=self.sequence_number)
		connection = engine.connect()
		with connection:
			connection.execute(sql_update_seq)
			
	@ContextProcessor
	def setup(self, context, *, engine):
		"""Setup the transformation.
		
		Connects to database if required. Upon successful connection it will
		pass the connection object to the next ContextProcessor.
		
		This is a ContextProcessor, it will be executed once at construction of
		the class. All @ContextProcessor functions will get executed in the
		order in which they are defined.
		
		Args:
			engine (service)
			
		Returns:
			connection
		"""
		
		self.sequence_number = None
		self.sequence_persisted = None
		connection = {}
		
		# validate options
		if self.persist_type != self.SEQ_PERSIST_DISABLED and self.source_value_tbl is not None:
			raise UnrecoverableError("[SEQ_{0}] ERROR: 'persist_type' and 'source_value_tbl' cannot be used together.".format(self.name))
		
		if self.persist_type == self.SEQ_PERSIST_FILE and self.persist_file is None:
			raise UnrecoverableError("[SEQ_{0}] ERROR: 'persist_type' set to SEQ_PERSIST_FILE, but 'persist_file' not provided.".format(self.name))
			
		if self.persist_type == self.SEQ_PERSIST_DB and (self.name is None or self.name == 'untitled'):
			raise UnrecoverableError("[SEQ_{0}] ERROR: 'persist_type' set to SEQ_PERSIST_DB, but 'name' not provided.".format(self.name))

		if self.source_value_tbl is not None and self.source_value_col is None:
			raise UnrecoverableError("[SEQ_{0}] ERROR: 'source_value_tbl' specified, but 'source_value_col' is empty.".format(self.name))

		if self.source_value_tbl is None and self.source_value_col is not None:
			raise UnrecoverableError("[SEQ_{0}] ERROR: 'source_value_col' specified, but 'source_value_tbl' is empty.".format(self.name))
		
		# retrieve high value from source
		if self.source_value_tbl is not None:
			self.sequence_persisted = self._get_sequence_from_db(engine, connection)
		
		# retrieve persisted values (non-database)
		if self.persist_type == self.SEQ_PERSIST_FILE:
			self.sequence_persisted = self._get_persisted_from_file()
		
		# setup connection
		if self.persist_type == self.SEQ_PERSIST_DB or self.source_value_tbl is not None:
			try:
				connection = engine.connect()
			except OperationalError as exc:
				raise UnrecoverableError("[LKP_{0}] ERROR: Could not create SQLAlchemy connection: {1}.".format(self.name, str(exc).replace('\n', ''))) from exc
		
		# retrieve persisted values (database)
		if self.persist_type == self.SEQ_PERSIST_DB:
			self.sequence_persisted = self._get_persisted_from_db(engine, connection)
		
		# return connection and enter transformation
		if connection:
			with connection:
				yield connection
		else:
			yield {}
		
		# teardown: persist values
		if self.persist_type != self.SEQ_PERSIST_DISABLED and self.sequence_number is not None:
			
			if self.persist_type == self.SEQ_PERSIST_DB:
				self._set_persisted_to_db(engine, connection)
			
			elif self.persist_type == self.SEQ_PERSIST_FILE:
				self._set_persisted_to_file()
				
			else:
				raise UnrecoverableError("[SEQ_{0}] ERROR: 'persist_type': invalid value.".format(self.name))
			
	
	def __call__(self, connection, context, d_row_in=None, *, engine):
		"""Row processor."""

		# initialize persisted
		if self.sequence_number is None and self.sequence_persisted is not None:
			self.sequence_number = self.sequence_persisted
		
		if d_row_in is None and self.generator:
			"""
			Generator mode
			"""	
			for i in range(self.generate):
			
				# initialize / cycle / increment
				if self.sequence_number is None:
					self.sequence_number = self.initial
					
				elif self.cycle and self.max is not None and self.sequence_number + self.increment > self.max:
					self.sequence_number = self.initial
					
				else:
					self.sequence_number = self.sequence_number + self.increment
				
				# send out row
				yield {self.sequence_key: self.sequence_number}
					
		else:
			"""
			Transformation mode
			"""
		
			# increment
			if self.sequence_number is not None:
				self.sequence_number += self.increment
			else:
				self.sequence_number = self.initial					

			# cycle
			if self.cycle and self.max is not None and self.sequence_number > self.max:
				self.sequence_number = self.initial
			
			# send out row
			yield {**d_row_in, self.sequence_key: self.sequence_number}
		
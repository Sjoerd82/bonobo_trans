"""
Author: Sjoerd Venema
Date: 2019-02-06
For licensing and version refer to package details.
"""

import logging

from bonobo.config import Configurable, ContextProcessor, Option, Service
from bonobo.config import use_context
from bonobo.errors import UnrecoverableError
from sqlalchemy import Column, Integer, MetaData, String, Table, func, select
from sqlalchemy.engine import create_engine

from bonobo_trans.logging import logger

@use_context
class Sorter(Configurable):
	"""The Sorter transformation sorts rows and can de-duplicate data.
	
	Configuration options:
	
		Required:
		-	sort_keys      (dict) {key:direction}
		
		Optional:
		-	name           (str)
		-	dedup          (bool) Default: False
		-	case_sensitive (bool) Default: False
		-	none_is_low    (bool) Default: False
		
	The 'sort_keys' option is a dictionary where the keys match the keys in the
	incoming row. The direction can either be 'ASC' or 'DESC'.	
		
	Args:
	Returns:
	"""
		
	engine = Service('sqlalchemy.engine')
	sort_keys      = Option(required=True, type=dict)
	name           = Option(required=False,type=str)
	dedup          = Option(required=False,type=bool, default=False)
	case_sensitive = Option(required=False,type=bool, default=False)
	none_is_Low    = Option(required=False,type=bool, default=False)
			
	@ContextProcessor
	def setup(self, context, *, engine):
		"""Connect to database.
		
		This is a ContextProcessor, it will be executed once at construction of
		the class. All @ContextProcessor functions will get executed in the
		order in which they are defined.
				
		Args:
			None
			
		Returns:
			None
		"""
		
		# Setup PANDAS / SQLMEM
		
		yield {}
		
		# teardown: yield last group			
	
	def __call__(self, connection, context, d_row_in, engine):
		"""Row processor."""
			
			yield {**d_row_in}
		
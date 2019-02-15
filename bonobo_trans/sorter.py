import logging
#from queue import Queue

from bonobo.config import Configurable, ContextProcessor, Option
from bonobo.config import use_context
from bonobo.errors import UnrecoverableError
import pandas as pd

from bonobo_trans.logging import logger

@use_context
class Sorter(Configurable):
	"""The Sorter transformation sorts rows and can de-duplicate data.
	
	.. admonition:: Configuration options
	
		**Required:**
		
			-	**keys_sort**       *(dict) {key:direction}*
		
		*Optional:*
		
			-	**name**            *(str)*
			-   **distinct**        *(int)*  Default: SRT_DUP_KEEP
			-   **keys_dedup**      *(list of str)*
			-	**case_sensitive**  *(bool)* Default: False
			-	**null_is_last**    *(bool)* Default: True
		
	**Description of the options:**
	
		keys_sort
			The ``sort_keys`` option is a dictionary where the keys refer to the keys
			in the incoming row. The direction indicates an ascending or descending
			sort.
			
			Direction can be one of the following:
			
				-	'ASC', 'ASCENDING', True, 1
				-	'DESC', 'DESCENDING', False, any number except 1
			
			Example::
			
				{'year':'ASC', 'month':'DESC', 'day':'ASC'}
		
		name
			Name of the transformation. Mainly used for identification in logging.
		
		distinct, keys_dedup
			The sorter transformation allows for removal of duplicate rows. There
			are different strategies to choose from:

			====================  =================================
			``distinct``          Description
			--------------------  ---------------------------------
			SRT_DUP_KEEP          Don't remove duplicates
			SRT_DUP_DISTINCT_ROW  Remove identical rows
			SRT_DUP_KEY_FIRST     Remove duplicate key, keep first
			SRT_DUP_KEY_LAST      Remove duplicate key, keep last
			====================  =================================
			
			By default duplicates are not removed (SRT_DUP_KEEP).
			
			**SRT_DUP_DISTINCT_ROW**
			
			Remove identical rows. This is similar to the SQL "DISTINCT"
			keyword. This setting will remove rows in which all rows are similar.
			
			**SRT_DUP_KEY_FIRST, SRT_DUP_KEY_LAST**
			
			Remove rows that have duplicate keys. This behaviour is more akin to
			an aggregator's FIRST and LAST-functions. It will remove rows with an
			identical key. You can specify to keep the first or last row.
			
			You can specify the de-duplication key as a subset of the sort key
			using then ``keys_dedup``-option. It accepts a list of keys (str). If
			you don't specify a 'keys_dedup' the first row will be kept, but this
			will give you less control and security as it will depend on how the
			rows enter this transformation.
			
			Example::
			
				'distinct'   = SRT_DUP_KEY_FIRST
				'keys_sort'  = {'year':'ASC', 'month':'ASC', 'day':'ASC'}
				'keys_dedup' = ['year', 'month']
			
				Input rows:
					2019,02,15,'Friday'
					2019,02,16,'Saturday'
					2019,02,17,'Sunday'
					
				Output rows:
					2019,02,15,'Friday'		
		
		case_sensitive
			TODO!
			
		null_is_last
			This option will determine if the None/Null will be on top or on
			bottom of the sorted output. By default it's True and the None
			value will be on the bottom.
		
	ToDo:
		*	[Q] (How) could we create a Deduplicator Class transformation as subclass of the sorter?
			Would that be nice?
		
	Args:
		*	**d_row_in** *(dict)*

		d_row_in is a dictonary containing row data.
		
	Returns:
		*	**d_row_out** *(dict)*
		
		d_row_out contains all the keys of the incoming	dictionary without
		any changes or additions.
		
		Only the order of the rows will change.
			
	"""
	
	SRT_DUP_KEEP         = 0
	SRT_DUP_DISTINCT_ROW = 1
	SRT_DUP_KEY_FIRST    = 2
	SRT_DUP_KEY_LAST     = 3
	
	keys_sort      = Option(required=True,  type=dict)
	name           = Option(required=False, type=str)
	distinct       = Option(required=False, type=int,  default=SRT_DUP_KEEP)
	keys_dedup     = Option(required=False, type=list)
	case_sensitive = Option(required=False, type=bool, default=False)
	null_is_last   = Option(required=False, type=bool, default=True)
	
	@ContextProcessor
	def buffer(self, context):
		"""
		
		This is a ContextProcessor, it will be executed at construction of the class.
		All @ContextProcessor functions will get executed in the
		order in which they are defined.
				
		Args:
			None
			
		Returns:
			row
		"""
		
		# validate options
		if not self.distinct in (self.SRT_DUP_KEEP, self.SRT_DUP_DISTINCT_ROW, self.SRT_DUP_KEY_FIRST, self.SRT_DUP_KEY_LAST):
			raise UnrecoverableError("[SRT_{0}] ERROR: 'distinct' Invalid value.".format(self.name))

		if self.keys_dedup is not None and self.distinct not in (self.SRT_DUP_KEY_FIRST, self.SRT_DUP_KEY_LAST):
			logger.warning("[SRT_{}] 'keys_dedup' is only useful when 'distinct' set to either SRT_DUP_KEY_FIRST or SRT_DUP_KEY_LAST.".format(self.name))
			
		# prepare list of keys and direction
		sort_keys = []
		sort_dirs = []
		for sort_key, sort_dir in self.keys_sort.items():
			# todo: check if key is in row -- or check this in the __call__? -- if not, add as empty??
			
			if isinstance(sort_dir, bool):
				direction = sort_dir
			elif isinstance(sort_dir, int) and sort_dir == 1:
				direction = True
			elif isinstance(sort_dir, int):
				direction = False
			elif isinstance(sort_dir, str) and (sort_dir.upper() == 'ASC' or sort_dir.upper() == 'ASCENDING'):
				direction = True
			elif isinstance(sort_dir, str) and (sort_dir.upper() == 'ASC' or sort_dir.upper() == 'DESCENDING'):
				direction = False
			else:
				raise UnrecoverableError("[SRT_{0}] ERROR: 'keys_sort' Invalid sort direction: {1}; please use 'ASC' or 'DESC'.".format(self.name, sort_dir))
			
			sort_keys.append(sort_key)
			sort_dirs.append(direction)
	
		# validate 'keys_dedup'
		if self.keys_dedup:
			for dedup_key in self.keys_dedup:
				if dedup_key not in sort_keys:
					raise UnrecoverableError("[SRT_{0}] ERROR: 'keys_dedup' Key '{1}' not part of the 'keys_sort'.".format(self.name, dedup_key))
			
		# prepare NULL order
		if self.null_is_last:
			na_position='last'
		else:
			na_position='first'

		dup_key_keep = None
		if self.distinct == self.SRT_DUP_KEY_FIRST:
			dup_key_keep = 'first'
		elif self.distinct == self.SRT_DUP_KEY_LAST:
			dup_key_keep = 'last'
		
		# create buffer
		buffer = yield []
		
		# Teardown; all rows received...
		df_sorter = pd.DataFrame(buffer)
		df_sorter.sort_values(sort_keys, ascending=sort_dirs, na_position=na_position, inplace=True)
		
		# apply distinct
		if self.distinct == self.SRT_DUP_DISTINCT_ROW:
			df_sorter.drop_duplicates(inplace=True)
		elif self.distinct in (self.SRT_DUP_KEY_FIRST, self.SRT_DUP_KEY_LAST):
			df_sorter.drop_duplicates(subset=['A', 'B'], keep=dup_key_keep, inplace=True)
		
		# replace nan with None
		df_sorter_none = df_sorter.where((pd.notnull(df_sorter)), None) # , inplace=True not possible
		
		# send the rows out
		for ix, row in df_sorter_none.iterrows():
			d_row_out = {**row}
			context.send(d_row_out)
	
	def __call__(self, buffer, context, d_row_in):
		buffer.append(d_row_in)
		

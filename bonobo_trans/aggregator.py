import logging
from queue import Queue

from bonobo.config import Configurable, ContextProcessor, Option, Service
from bonobo.config import use_context
from bonobo.errors import UnrecoverableError

from bonobo_trans.logging import logger

@use_context
class Aggregator(Configurable):
	"""The Aggregator transformation provides aggregates functions on row data.
	
	.. important::
		All input *MUST BE SORTED* prior to sending it to this transformation!
	
	.. admonition:: Configuration options
		
		**Required:**
		
			-	**group**         *(list of str)*
			-	**aggregations**  *(list of dict)*
		
		*Optional:*
	
			-	**name**          *(str)*
			-	**null_is_zero**  *(bool)* Default: False
			-	**return_all**    *(bool)* Default: False
			
		
	**Option descriptions:**
		
		name
			Name of the transformation, for identification and logging purposes.	
			
		null_is_zero
			Set to true to treat NULL as zero.
			
		return_all
			Set to True to return all incoming rows. If False (default) the
			transformation will only return the key group.
			
		group	
			A list of the columns to aggregate on. The incoming rows must have been
			sorted on these keys.
			
		aggregations	
			A list of the aggregations (dict). An aggregation is a dictionary
			object of which the key is the output key to be appended to the
			outgoing row and the value is the aggregation, and must be one of the
			following:
			
			- AGG_MAX, AGG_MIN
			- AGG_FIRST, AGG_LAST
			- AGG_AVG
			- AGG_MEDIAN
			- AGG_PERCENTILE
			- AGG_SUM
			- AGG_STDDEV
			- AGG_VARIANCE
			- AGG_COUNT
			
			**MAX, MIN**
			
			MAX returns the highest number, newest date or alphabetically last string.
			MIN does the reverse.
			
			Example:
			
				``{ 'high_key': { AGG_MAX: 'col1' } }``
			
			**FIRST, LAST**
			
			FIRST returns the first row of the group. LAST does the reverse.
		
			Example:
			
				``{ 'last_key': { AGG_LAST: 'col1' } }``
			
			**AVG**
			
			Returns the average of all numeric values in specified column in the
			group.
			
			Example:
			
				``{ 'sales_avg': { AGG_AVG: 'sales_usd' } }``
			
			**MEDIAN**
			
			Returns the median of all numeric values in specified column in the
			group. If there is an even number of values, the median is the average
			of the middle two values when all values are placed ordinally on a
			number line. If there is an odd number of values, the median is the
			middle number.
			
			Example:
			
				``{ 'sales_med': { AGG_MED: 'sales_usd' } }``
				
			**PERCENTILE**
			
			Calculates the value that falls at a given percentile in specified
			column in the group. Column must be numeric.

			Example:
			
				``{ 'percentile': { AGG_PERCENTILE: 'transaction_id', 'percentile': 25 } }``
			
			**SUM**
			
			Returns the total of all numeric values in specified column in the
			group.
			
			Example:
			
				``{ 'sales_total': { AGG_SUM: 'sales_usd' } }``
			
			**STDDEV**
			
			Returns the standard deviation of the numeric values of the specifed
			column in the group. STDDEV is used to analyze statistical data.

			Example:
			
				``{ 'score_stdev': { AGG_STDDEV: 'score' } }``
			
			**VARIANCE**
			
			Returns the variance of the numeric values of the specified column in
			the group. VARIANCE is used to analyze statistical data.
			
			Example:
			
				``{ 'score_var': { AGG_VARIANCE: 'score' } }``
			
			**COUNT**
			
			Returns the number of records in the group_count
			
			Example:
			
				``{ 'nr_of_transactions': { AGG_COUNT: 'transaction_id' } }``
			
			
	Todo:
		*	Filter conditions. E.g. SUM where summed > 100.
	
	Args:
		*	**d_row_in** *(dict)*

		d_row_in is a dictonary containing sorted row data.
		
	Returns:
		*	**d_row_out** *(dict)*
		
		d_row_out contains all the keys of the incoming	dictionary plus the
		aggregations. Any keys with the same name as the specified agg-
		regation keys will be overwritten.
			
		If 'return_all' is specified, all rows will be returned, else only
		one row per group.
		
	"""
	
	AGG_MIN        = 0
	AGG_MAX        = 1
	AGG_FIRST      = 2
	AGG_LAST       = 3
	AGG_AVG        = 4
	AGG_MEDIAN     = 5
	AGG_PERCENTILE = 6
	AGG_SUM        = 7
	AGG_STDDEV     = 8
	AGG_VARIANCE   = 9
	AGG_COUNT      = 10

	buffer_size = 1000 #Option(int, required=False, default=1000)  # type: int
	
	group        = Option(required=True,  type=list)
	aggregations = Option(required=True,  type=dict)
	name         = Option(required=False, type=str,  default="untitled")
	null_is_zero = Option(required=False, type=bool, default=False)
	return_all   = Option(required=False, type=bool, default=False)	
			
	@ContextProcessor
	def create_buffer(self, context):
		"""Setup the transformation.
		
		This is a ContextProcessor, it will be executed once at construction of
		the class. All @ContextProcessor functions will get executed in the
		order in which they are defined.
		
		Args:
			None
			
		Returns:
			?
		"""
		
		prev_row = {}
		sum = 0
		
		# fill buffer
		buffer = yield Queue()
		
		# process buffer
		for row in self.commit(buffer, force=True): 
			
			if prev_row and row['id'] != prev_row['id']:
				yield_row = {**prev_row, 'sum': sum}
				prev_row = row
				sum = row['value']
				context.send(yield_row)
				
			else:
				sum += int(row['value'])
				prev_row = row

		# teardown; send out last row
		context.send({**prev_row, 'sum': sum})
	
	def __call__(self, buffer, context, d_row_in):
		buffer.put(d_row_in)
		yield from self.commit(buffer)

		
	def commit(self, buffer, force=False):
		if force or (buffer.qsize() >= self.buffer_size):
			while buffer.qsize() > 0:
				try:
					yield buffer.get()
				
				except Exception as exc:
					yield exc
					
	
	
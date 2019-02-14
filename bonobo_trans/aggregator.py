import copy
import logging
from queue import Queue
import statistics

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
	
	group        = Option(required=True,  type=list)
	aggregations = Option(required=True,  type=dict)
	name         = Option(required=False, type=str,  default="untitled")
	null_is_zero = Option(required=False, type=bool, default=False)
	return_all   = Option(required=False, type=bool, default=False)	
	
	def _aggregate(self, group_rows):

		agg_out_columns = {}
	
		# list of values
		agg_lov = []
		for group_row in group_rows:
			agg_lov.append(group_row['qtty'])	#TODO!!!!!!!!!!!!!!!!!

		#if needs_sum:
		self.agg_sum = sum(agg_lov)

		# calculations
		for agg_col_out, agg_spec in self.aggregations.items():
			for agg_type, agg_col in agg_spec.items():
			
				if agg_type == self.AGG_MIN:
					agg_out_columns[agg_col_out] = min(agg_lov)

				elif agg_type == self.AGG_MAX:
					agg_out_columns[agg_col_out] = max(agg_lov)
					
				elif agg_type == self.AGG_FIRST:
					agg_out_columns[agg_col_out] = group_rows[0][agg_col]
					
				elif agg_type == self.AGG_LAST:
					agg_out_columns[agg_col_out] = group_rows[-1][agg_col]
					
				elif agg_type == self.AGG_AVG:
					agg_out_columns[agg_col_out] = self.agg_sum / self.agg_count
					
				elif agg_type == self.AGG_MEDIAN:
					agg_out_columns[agg_col_out] = statistics.median(agg_lov)
					
				elif agg_type == self.AGG_PERCENTILE:
					agg_out_columns[agg_col_out] = None #TODO
					
				elif agg_type == self.AGG_SUM:
					agg_out_columns[agg_col_out] = self.agg_sum
					
				elif agg_type == self.AGG_STDDEV:
					agg_out_columns[agg_col_out] = statistics.stdev(agg_lov)
					
				elif agg_type == self.AGG_VARIANCE:
					agg_out_columns[agg_col_out] = statistics.variance(agg_lov)
					
				elif agg_type == self.AGG_COUNT:
					agg_out_columns[agg_col_out] = self.agg_count
					
		return agg_out_columns

	
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
		
		self.agg_sum = 0
		self.agg_count = 0
				
		key_row = []
		key_prev_row = []

		group_rows = []
		
		needs_count = True
		needs_sum = True
		needs_lov = True
		
		# validate aggregations
		for agg_name, agg_spec in self.aggregations.items():

			if not isinstance(agg_spec, dict):
				raise UnrecoverableError("[AGG_{0}] ERROR: 'aggregations': Invalid aggregation specification: {1}. Must be a dictionary.".format(self.name, agg_spec))
			
			# disabled for now
			for agg_type, agg_col in agg_spec.items():
				pass
			#	if agg_type not in (self.AGG_SUM, self.AGG_STDDEV):
			#		raise UnrecoverableError("[AGG_{0}] ERROR: 'aggregations': Invalid aggregation specification: ({1}).".format(self.name, agg_name))

			
		
		# fill buffer
		buffer = yield Queue()
		
		# process buffer
		for row in self.commit(buffer, force=True):
			
			key_row = []
			for col in self.group:
				key_row.append(row[col])
			
			if not key_prev_row:
				#
				# FIRST ROW
				#
				group_rows.append({**row})
				# for agg in aggregations
				
				# check if all required columns are present in row (we only check the first row)
				for agg_name, agg_spec in self.aggregations.items():
					for agg_type, agg_col in agg_spec.items():
						if agg_col not in row:
							raise UnrecoverableError("[AGG_{0}] ERROR: 'aggregations': Column '{1}' not found in row.".format(self.name, agg_col))
							
						# TODO: optimize by checking in advance if an LOV, SUM or COUNT is required
						# for now, let's always calculate them:
						#needs_count = False
						#needs_sum = False
						#needs_lov = False

				
			if key_prev_row and key_prev_row != key_row:
				#
				# KEY CHANGE
				#
				
				agg_out_columns = self._aggregate(group_rows)
				
				# add aggregations to row(s)
				if self.return_all:
					for group_row in group_rows:
						group_row.update(agg_out_columns) # will this perform optimally?
					yield_rows = copy.deepcopy(group_rows) # deepcopy, because group_rows will soon be reset to []
					
				else:
					yield_rows = [{**group_rows[-1], **agg_out_columns}]
				
				#
				# restart new group
				#
				
				# first row:
				group_rows = [{**row}]
				
				if needs_count:
					self.agg_count += 1
				
				# create the "previous row key"
				key_prev_row = []
				for col in self.group:
					key_prev_row.append(row[col])
				
				#
				# yield one / all aggregated record
				#
				for yield_row in yield_rows:
					context.send(yield_row)
				
			else:
				#
				# same group
				#
				group_rows.append({**row})
								
				# increase aggregations
				if needs_count:
					self.agg_count += 1
								
				# create the "previous row key"
				key_prev_row = []
				for col in self.group:
					key_prev_row.append(row[col])
		
		
		# teardown; send out last row
		
		# calculations
		agg_out_columns = self._aggregate(group_rows)
		
		# add aggregations to row(s)
		if self.return_all:
			for group_row in group_rows:
				group_row.update(agg_out_columns) # will this perform optimally?
			yield_rows = group_rows
			
		else:
			yield_rows = [{**group_rows[-1], **agg_out_columns}]
					
		# yield one / all aggregated record
		for yield_row in yield_rows:
			context.send(yield_row)
	
	
	def __call__(self, buffer, context, d_row_in):
		buffer.put(d_row_in)
		#yield from self.commit(buffer)
		
	def commit(self, buffer, force=False):
		#if force or (buffer.qsize() >= self.buffer_size):
		while buffer.qsize() > 0:
			try:
				yield buffer.get()
			
			except Exception as exc:
				yield exc

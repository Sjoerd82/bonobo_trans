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
		
			-	**group**           *(list of str)*
			-	**aggregations**    *(list of dict)*
		
		*Optional:*
	
			-	**name**            *(str)*
			-	**null_is_zero**    *(bool)* Default: False
			-	**return_all_rows** *(bool)* Default: False
			
		
	**Option descriptions:**
		
		name
			Name of the transformation, for identification and logging purposes.	
			
		null_is_zero
			Set to true to treat NULL as zero.
			
		return_all_rows
			Set to True to return all incoming rows. If False (default) the
			transformation will only return the key group on which was aggregated.
			
		return_all_cols
			This setting will be ignored when ``return_all_rows`` is True.
			
			When False the transformation will return the key columns plus the
			requested aggregations. When set to True, all columns will be
			returned, the values for these columns will be of the last row of
			the group.
			
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
			- AGG_MEAN, AGG_MEAN_HARMONIC
			- AGG_MEDIAN, AGG_MEDIAN_HIGH, AGG_MEDIAN_LOW
			- AGG_PERCENTILE
			- AGG_SUM
			- AGG_STDDEV_S, AGG_STDDEV_P
			- AGG_VARIANCE_S, AGG_VARIANCE_P
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
			
			**MEAN, HARMONIC MEAN**
			
			MEAN returns the average of all numeric values in specified column
			in the group. MEAN_HARMONIC returns the harmonic (subcontrary)
			mean. Values less than zero are not allowed for the harmonic mean.
			
			Example:
			
				``{ 'sales_avg': { AGG_MEAN: 'sales_usd' } }``
			
			**MEDIAN (HIGH/LOW)**
			
			MEDIAN returns the median of all numeric values in specified column
			in the group. If there is an *odd* number of values, the median is
			the middle number. If there is an *even* number of values, the
			median is the average of the middle two values when all values are
			placed ordinally on a number line. Use MEDIAN_HIGH or MEDIAN_LOW to
			not return the average middle (in case of an even number of values,
			but instead return the highest or lowest of the two middle values.
			
			Example:
			
				``{ 'sales_med': { AGG_MED: 'sales_usd' } }``
			
			**MODE**
			
			MODE returns the most common value, if any. If there is no exact
			most common value, None is returned. Values can be non-numeric.
			
			**PERCENTILE** (not implemented yet)
			
			Calculates the value that falls at a given percentile in specified
			column in the group. Column must be numeric.

			Example:
			
				``{ 'percentile': { AGG_PERCENTILE: 'transaction_id', 'percentile': 25 } }``
			
			**SUM**
			
			Returns the total of all numeric values in specified column in the
			group.
			
			Example:
			
				``{ 'sales_total': { AGG_SUM: 'sales_usd' } }``
			
			**STDDEV (SAMPLE/POPULATION)**
			
			Returns the standard deviation of the numeric values of the
			specifed column in the group. STDDEV is used to analyze statistical
			data. This aggregation will return None if less than two values
			provided. Use STDDEV_S or STDDEV_P to return either a *sample* or
			*population* standard deviation.

			Example:
			
				``{ 'score_stdev': { AGG_STDDEV_S: 'score' } }``
			
			**VARIANCE (SAMPLE/POPULATION)**
			
			Returns the variance of the numeric values of the specified column
			in the group. VARIANCE is used to analyze statistical data. This
			aggregation will return None if less than two values provided.
			Use VARIANCE_S or VARIANCE_P to return either a *sample* or
			*population* standard deviation.
			
			Example:
			
				``{ 'score_var': { AGG_VARIANCE_S: 'score' } }``
			
			**COUNT**
			
			Returns the number of records in the group. The specification is
			slightly different, instead of a dictionary, you only specify the
			aggregation, as shown in the exmple:
			
			Example:
			
				``{ 'nr_of_transactions': AGG_COUNT }``
			
			
	Todo:
		*	Filter conditions. E.g. SUM where summed > 100.
		*	ABS(), ROUND() options
		*	Do we need a null_is_last option for the first and last functions?
		*	Percentile
	
	Args:
		*	**d_row_in** *(dict)*

		d_row_in is a dictonary containing sorted row data.
		
	Returns:
		*	**d_row_out** *(dict)*
		
		d_row_out contains all the keys specified in the ``group`` and
		``aggregations`` options. If ``return_all_cols`` is set to True it will
		also include the non-key columns of incoming dictionary.
		
		.. attention::
			Any keys with the same name as the specified aggregation keys will
			be overwritten.
			
		If ``return_all_rows`` is specified, all rows will be returned, else only
		one row per group.
		
	"""
	
	AGG_MIN           = 1
	AGG_MAX           = 2
	AGG_FIRST         = 3
	AGG_LAST          = 4
	AGG_MEAN          = 5
	AGG_MEAN_HARMONIC = 6
	AGG_MEDIAN        = 7
	AGG_MEDIAN_HIGH   = 8
	AGG_MEDIAN_LOW    = 9
	AGG_MODE          = 10
	AGG_PERCENTILE    = 11
	AGG_SUM           = 12
	AGG_STDDEV_P      = 13
	AGG_STDDEV_S      = 14
	AGG_VARIANCE_P    = 15
	AGG_VARIANCE_S    = 16
	AGG_COUNT         = 17

	group           = Option(required=True,  type=list)
	aggregations    = Option(required=True,  type=dict)
	name            = Option(required=False, type=str,  default="untitled")
	null_is_zero    = Option(required=False, type=bool, default=False)
	return_all_rows = Option(required=False, type=bool, default=False)
	return_all_cols = Option(required=False, type=bool, default=False)
	
	def _aggregate(self, group_rows):

		agg_out_columns = {}
		agg_lov = {}
		
		# list of values
		for agg_col_out, agg_spec in self.aggregations.items():
			if isinstance(agg_spec, dict):
				for agg_type, agg_col in agg_spec.items():
					# create List of Values (LOV) for aggregations that need it
					
					# numeric
					if agg_type in (  self.AGG_MEDIAN, self.AGG_MEDIAN_HIGH, self.AGG_MEDIAN_LOW, self.AGG_STDDEV_P
									, self.AGG_STDDEV_S, self.AGG_VARIANCE_P, self.AGG_VARIANCE_S, self.AGG_MEAN
									, self.AGG_MEAN_HARMONIC, self.AGG_SUM):
						agg_lov[agg_col_out] = []
						for group_row in group_rows:
							if isinstance(group_row[agg_col], (int, float)):
								agg_lov[agg_col_out].append(group_row[agg_col])
							elif self.null_is_zero and group_row[agg_col] is None:
								agg_lov[agg_col_out].append(0)
								
					# alphanumeric, dates
					elif agg_type in (self.AGG_MIN, self.AGG_MAX, self.AGG_MODE):
						agg_lov[agg_col_out] = []
						for group_row in group_rows:
							agg_lov[agg_col_out].append(group_row[agg_col])
				
		# calculations
		for agg_col_out, agg_spec in self.aggregations.items():
			
			if agg_spec == self.AGG_COUNT:
				agg_out_columns[agg_col_out] = self.counter
				
			elif isinstance(agg_spec, dict):
				for agg_type, agg_col in agg_spec.items():
				
					if agg_type == self.AGG_MIN:
						agg_out_columns[agg_col_out] = min(agg_lov[agg_col_out])

					elif agg_type == self.AGG_MAX:
						agg_out_columns[agg_col_out] = max(agg_lov[agg_col_out])
						
					elif agg_type == self.AGG_FIRST:
						agg_out_columns[agg_col_out] = group_rows[0][agg_col]
						
					elif agg_type == self.AGG_LAST:
						agg_out_columns[agg_col_out] = group_rows[-1][agg_col]
						
					elif agg_type == self.AGG_MEAN:
						agg_out_columns[agg_col_out] = statistics.mean(agg_lov[agg_col_out])

					elif agg_type == self.AGG_MEAN_HARMONIC:
						agg_out_columns[agg_col_out] = statistics.harmonic_mean(agg_lov[agg_col_out])

					elif agg_type == self.AGG_MEDIAN:
						agg_out_columns[agg_col_out] = statistics.median(agg_lov[agg_col_out])

					elif agg_type == self.AGG_MEDIAN_HIGH:
						agg_out_columns[agg_col_out] = statistics.median_high(agg_lov[agg_col_out])

					elif agg_type == self.AGG_MEDIAN_LOW:
						agg_out_columns[agg_col_out] = statistics.median_low(agg_lov[agg_col_out])

					elif agg_type == self.AGG_MODE:
						try:
							agg_out_columns[agg_col_out] = statistics.mode(agg_lov[agg_col_out])
						except statistics.StatisticsError:
							# no exact mode could be determined
							agg_out_columns[agg_col_out] = None

					elif agg_type == self.AGG_PERCENTILE:
						agg_out_columns[agg_col_out] = None #TODO
						
					elif agg_type == self.AGG_SUM:
						agg_out_columns[agg_col_out] = sum(agg_lov[agg_col_out])
					
					elif len(agg_lov[agg_col_out]) > 1 and agg_type == self.AGG_STDDEV_P:
						agg_out_columns[agg_col_out] = statistics.pstdev(agg_lov[agg_col_out])
						
					elif len(agg_lov[agg_col_out]) > 1 and agg_type == self.AGG_STDDEV_S:
						agg_out_columns[agg_col_out] = statistics.stdev(agg_lov[agg_col_out])

					elif len(agg_lov[agg_col_out]) > 1 and agg_type == self.AGG_VARIANCE_P:
						agg_out_columns[agg_col_out] = statistics.pvariance(agg_lov[agg_col_out])
							
					elif len(agg_lov[agg_col_out]) > 1 and agg_type == self.AGG_VARIANCE_S:
						agg_out_columns[agg_col_out] = statistics.variance(agg_lov[agg_col_out])
							
					elif len(agg_lov[agg_col_out]) < 2 and agg_type in (self.AGG_STDDEV_P, self.AGG_STDDEV_S, self.AGG_VARIANCE_P, self.AGG_VARIANCE_S):
						agg_out_columns[agg_col_out] = None
		
					#elif agg_type == self.AGG_COUNT:
					#	agg_out_columns[agg_col_out] = self.counter

					"""
					debugging:
					if agg_type == 0:
						agg_out_columns[agg_col_out+'agg_min'] = min(agg_lov[agg_col_out])
						agg_out_columns[agg_col_out+'agg_max'] = max(agg_lov[agg_col_out])
						agg_out_columns[agg_col_out+'agg_first'] = group_rows[0][agg_col]
						agg_out_columns[agg_col_out+'agg_last'] = group_rows[-1][agg_col]
						agg_out_columns[agg_col_out+'agg_avg'] = sum(agg_lov[agg_col_out]) / self.counter
						agg_out_columns[agg_col_out+'agg_median'] = statistics.median(agg_lov[agg_col_out])
						agg_out_columns[agg_col_out+'agg_median_h'] = statistics.median_high(agg_lov[agg_col_out])
						agg_out_columns[agg_col_out+'agg_median_l'] = statistics.median_low(agg_lov[agg_col_out])
						agg_out_columns[agg_col_out+'agg_percentile'] = None #TODO
						agg_out_columns[agg_col_out+'agg_sum'] = sum(agg_lov[agg_col_out])
						
						if len(agg_lov[agg_col_out]) < 2:
							agg_out_columns[agg_col_out+'agg_stddev_p'] = None
							agg_out_columns[agg_col_out+'agg_stddev_s'] = None
							agg_out_columns[agg_col_out+'agg_variance'] = None
							agg_out_columns[agg_col_out+'agg_variance'] = None
						else:
							agg_out_columns[agg_col_out+'agg_stddev_p'] = statistics.pstdev(agg_lov[agg_col_out])
							agg_out_columns[agg_col_out+'agg_stddev_s'] = statistics.stdev(agg_lov[agg_col_out])
							agg_out_columns[agg_col_out+'agg_variance_p'] = statistics.pvariance(agg_lov[agg_col_out])
							agg_out_columns[agg_col_out+'agg_variance_s'] = statistics.variance(agg_lov[agg_col_out])
							
						agg_out_columns[agg_col_out+'agg_count'] = self.counter
					"""
						
		return agg_out_columns

	
	@ContextProcessor
	def create_buffer(self, context):
		
		self.counter = 0
		key_row = []
		key_prev_row = []
		group_rows = []
				
		# validate aggregations
		for agg_name, agg_spec in self.aggregations.items():

			if agg_spec != self.AGG_COUNT and not isinstance(agg_spec, dict):
				raise UnrecoverableError("[AGG_{0}] ERROR: 'aggregations': Invalid aggregation specification: {1}. Must be a dictionary or AGG_COUNT.".format(self.name, agg_spec))
			
			# check aggregation definition -- disabled for now
			#for agg_type, agg_col in agg_spec.items():
			#	pass
			#	if agg_type not in (self.AGG_SUM, self.AGG_STDDEV):
			#		raise UnrecoverableError("[AGG_{0}] ERROR: 'aggregations': Invalid aggregation specification: ({1}).".format(self.name, agg_name))
		
		# fill buffer
		buffer = yield Queue()
		
		# process buffer
		for row in self.commit(buffer):
			
			key_row = []
			for col in self.group:
				key_row.append(row[col])
			
			if not key_prev_row:
				#
				# FIRST ROW
				#
			
				# check if all required columns are present in row (we only check the first row)
				for agg_name, agg_spec in self.aggregations.items():
					if isinstance(agg_spec, dict):
						for agg_type, agg_col in agg_spec.items():
							if agg_col not in row:
								raise UnrecoverableError("[AGG_{0}] ERROR: 'aggregations': Column '{1}' not found in row.".format(self.name, agg_col))

				
			if key_prev_row and key_prev_row != key_row:
				#
				# KEY CHANGE
				#
				
				agg_out_columns = self._aggregate(group_rows)
				
				# add aggregations to row(s)
				if self.return_all_rows:
					for group_row in group_rows:
						group_row.update(agg_out_columns) # will this perform optimally?
					yield_rows = copy.deepcopy(group_rows) # deepcopy, because group_rows will soon be reset to []
				else:
					# return all or only key + aggregations
					if self.return_all_cols:
						yield_rows_k  = group_rows[-1]
					else:
						yield_rows_k = {k: v for k, v in group_rows[-1].items() if k in self.group}
						
					yield_rows = [{**yield_rows_k, **agg_out_columns}]
					
				#
				# restart new group
				#
				
				# first row:
				group_rows = [{**row}]
				self.counter = 1
				
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
				self.counter += 1
								
				# create the "previous row key"
				key_prev_row = []
				for col in self.group:
					key_prev_row.append(row[col])
		
		"""
		teardown; send out last row
		"""
		
		# calculations
		agg_out_columns = self._aggregate(group_rows)
		
		# add aggregations to row(s)
		if self.return_all_rows:
			for group_row in group_rows:
				group_row.update(agg_out_columns) # will this perform optimally?
			yield_rows = group_rows
			
		else:
			# return all or only key + aggregations
			if self.return_all_cols:
				yield_rows_k  = group_rows[-1]
			else:
				yield_rows_k = {k: v for k, v in group_rows[-1].items() if k in self.group}
				
			yield_rows = [{**yield_rows_k, **agg_out_columns}]
					
		# yield one / all aggregated record
		for yield_row in yield_rows:
			context.send(yield_row)
	
	
	def __call__(self, buffer, context, d_row_in):
		buffer.put(d_row_in)
	
	
	def commit(self, buffer):
		while buffer.qsize() > 0:
			try:
				yield buffer.get()
			
			except Exception as exc:
				yield exc
	

import logging

import bonobo
from bonobo.config import Configurable, Service, Option, use_context
from bonobo_trans.source import DbSource
from bonobo_trans.lookup import DbLookup
from bonobo_trans.sorter import Sorter
from bonobo_trans.aggregator import Aggregator
from bonobo_trans.sequencer import Sequencer
from bonobo_trans.target import DbTarget
import sqlalchemy

def get_services():
	return {
		  'sqlalchemy.engine':sqlalchemy.create_engine('sqlite:///paintstore.db')
		, 'fs.output': bonobo.open_fs()
	}

cfg_aggregations = {
	  'sum_qtty'     : {Aggregator.AGG_SUM:'qtty'}
	, 'count'        :  Aggregator.AGG_COUNT
	, 'min_volume_ml': {Aggregator.AGG_MIN:'volume_ml'}
	, 'max_volume_ml': {Aggregator.AGG_MAX:'volume_ml'}
	, 'mode_shade'   : {Aggregator.AGG_MODE:'shade'}
}

def get_graph():
	return bonobo.Graph(
		DbSource(table_name='sales'),
		#bonobo.Limit(20),
		DbLookup(table_name='color_group', comparison=[[{'shade':'shade'}]], ret_fields='color_group', multiple_match=DbLookup.LKP_MM_ANY),
		Sorter(keys_sort={'sales_date':'ASC', 'color_group':'ASC'}),
		Aggregator(group=['sales_date', 'color_group'], aggregations=cfg_aggregations, return_all_rows=False, null_is_zero=True),
		Sequencer(),
		DbTarget(table_name='sales_stats', truncate=True),
		#bonobo.PrettyPrinter(),
		#bonobo.CsvWriter("simple.csv", fs='fs.output'),
		)

if __name__ == '__main__':
	bonobo.run(get_graph(), services=get_services() )

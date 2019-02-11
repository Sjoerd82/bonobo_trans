#! python3

from sqlalchemy import select, or_, bindparam, create_engine, MetaData
import bonobo
from bonobo_trans import source, target

SQLITE_DB = 'sqlite:///sqlite_example.db'
engine    = create_engine(SQLITE_DB)
field1_id = None
sequence_id = 0

#-----------------------------------------------------------------------------
# source and target table definitions

meta = MetaData()
meta.reflect(bind=engine)

stg_table = meta.tables['staging']
tgt_table = meta.tables['target']

sources = {
	'staging_1' : {
			  'table_name'             : 'staging'
			, 'sql_select'             : select([stg_table], distinct=True).\
										 where(or_( stg_table.c.Field1 == bindparam('param_field1') \
												  , stg_table.c.Field1 >= 1100) ) #BUG!, this generates a bind variable, will not compare with actuall hardcoded value.
			, 'sql_select_bind_params' : {'param_field1': field1_id}
	},
	'staging_2' : {
			  'table_name'             : 'staging'
			, 'select_distinct'        : True
			, 'sql_select'             : None
	}
}

targets = {
	'target': {
			  'table_name'             : 'target'
			, 'sql_pre'                : tgt_table.delete()
		}
}

#-----------------------------------------------------------------------------

def t_transform(d_row_in):
	global sequence_id
	
	d_row_out = {**d_row_in}

	sequence_id += 1
	d_row_out['id'] = sequence_id
	d_row_out['Field1_int'] = int(d_row_out['Field1'])
	d_row_out['Field2_str'] = d_row_out['Field2']
	d_row_out['Fiedl3_data'] = d_row_out['Field3']
	
	return d_row_out

def get_services():
	return {
		'sqlalchemy.engine':create_engine(SQLITE_DB)
	}

def get_graph_stg():
	graph = bonobo.Graph()
	graph.add_chain(
		source.SourceQualifier(**sources['staging_1']),
		t_transform,
		target.LoadTarget(**targets['target']),
	)
	return graph

def main():
	field1_id = 1000
	bonobo.run(get_graph_stg(), services=get_services())

if __name__ == '__main__':
	main()
# ipython

from hypermorph import MIS
from hypermorph.data_graph import GData
from graph_tool.draw import graph_draw, arf_layout

mis = MIS(debug=1, rebuild=True, warning=False,
          net_name='demo_schema', net_format='gt', net_path='data/DemoData/Schemata')

# -------------------------------------------------------------------------------------------------------------------
# clear
# -------------------------------------------------------------------------------------------------------------------
# WHAT IF YOU CAN add data sets from any data resource all with the same simple command...

# Flat Files
mis.add('dataset', with_components=['tables', 'fields'], extra={'path': 'SupplierPartCatalog'},
        ctype='TSV', cname='Supplier Part Catalogue TSV flat files', alias='SPC',
        descr='Supplier, Part, Catalog and denormalized data')

# SQLite database tables and fields
mis.add('dataset', with_components=['tables', 'fields'], extra={'path': 'spc.sqlite', 'api': 'SQLAlchemy'},
        ctype='SQLite', cname='Supplier Part Catalogue in SQLite', alias='SPC',
        descr='It is a toy database with three tables that form a many to many relationship ')

# MySQL database tables and fields
mis.add('dataset', with_components=['tables', 'fields'],
        cname='Supplier Part Catalogue in MySQL', alias='SPC', ctype='MYSQL',
        extra={'host': 'localhost', 'port': 3306, 'user': 'demo', 'password': 'demo',
               'db': 'SPC', 'api': 'SQLAlchemy'},
        descr='It is a toy database with three tables that form a many to many relationship ')


# WHAT IF YOU CAN display the schema of these objects you created
mis.overview

# or view their components on a dataframe
mis.get(48).get_components().over('nid, dim3, dim2, ntype, parent, cname, alias, vtype').to_dataframe(index='dim3, dim2').out()

# or view their components as tuples
mis.get(48).get_components().over('nid, dim3, dim2, ntype, parent, cname, alias, vtype').to_tuples().out()

# or as objects
mis.get(48).get_components().to_nodes().out()

# or in a key:value dictionary
mis.at(1, 363, 0).get_components().to_dict(key_column='nid', value_columns='dim3, dim2, ntype, parent, cname, alias, vtype').out()

# or in records (list of dictionaries)
mis.at(1, 363, 1).get_fields().over('nid, dim3, cname, ctype, vtype').to_dict_records().out()

# or even plotted on a hypergraph
mis.at(1, 363, 1).get_fields().to_hypergraph().out(edge_label='hasField')

# . .

# -------------------------------------------------------------------------------------------------------------------
# clear
# -------------------------------------------------------------------------------------------------------------------
# what if EVERYTHING you see on the schema is a node of a graph
mis.at(1, 363, 1)

# with SchemaLink(s) to outward nodes
mis.at(1, 363, 1).out_nodes

mis.at(1, 363, 1).out_links

# and with SchemaLink(s) to inward nodes
mis.at(1, 363, 1).in_nodes

mis.at(1, 363, 1).in_nodes

# -------------------------------------------------------------------------------------------------------------------
# clear
# -------------------------------------------------------------------------------------------------------------------
# WHAT IF YOU CAN add a DataModel in the same way you did with data resources
dm = mis.add('datamodel', cname='Supplier Part Catalog', alias='SPC',
             descr='Model with three entities that represent relations (tables) in a relational database')

# and then add Entity objects to your DataModel
mis.add('entity', datamodel=dm, cname='Supplier', alias='SUP', descr='The Supplier Entity of the data model')
mis.add('entity', datamodel=dm, cname='Part',     alias='PRT', descr='The Part Entity of the data model')
mis.add('entity', datamodel=dm, cname='Catalog',  alias='CAT', descr='The Catalog Entity of the data model')

# and Attribute objects this way
mis.add('attribute', datamodel=dm, entalias='SUP', cname='sname',    alias='s_name',    descr='supplier name',
        extra={'vtype': 'utf8', 'missing': 0})
mis.add('attribute', datamodel=dm, entalias='SUP', cname='saddress', alias='s_address', descr='supplier address',
        extra={'vtype': 'utf8', 'missing': 1})
mis.add('attribute', datamodel=dm, entalias='SUP', cname='scity',    alias='s_city',    descr='supplier city',
        extra={'vtype': 'utf8', 'enum': 1, 'missing': 1})
mis.add('attribute', datamodel=dm, entalias='SUP', cname='scountry', alias='s_country', descr='supplier country',
        extra={'vtype': 'utf8', 'enum': 1, 'missing': 1})
mis.add('attribute', datamodel=dm, entalias='SUP', cname='sstatus',  alias='s_status',  descr='supplier status',
        extra={'vtype': 'bool_', 'enum': 1, 'missing': 1})

# or that way
dm.add_attribute(entalias='PRT', cname='pname',    alias='p_name',    descr='part name',   extra={'vtype': 'utf8', 'missing': 0})
dm.add_attribute(entalias='PRT', cname='pcolor',   alias='p_color',   descr='part color',  extra={'vtype': 'utf8', 'enum': 1, 'missing': 1})
dm.add_attribute(entalias='PRT', cname='pweight',  alias='p_weight',  descr='part weight', extra={'vtype': 'float32', 'missing': 1})
dm.add_attribute(entalias='PRT', cname='punit',    alias='p_unit',    descr='part unit',   extra={'vtype': 'utf8', 'enum': 1, 'missing': 1})

# it is simple and fun isn't it
mis.add('attribute', datamodel=dm, entalias='CAT', cname='cprice',    alias='c_price',    descr='catalog price',
        extra={'vtype': 'float32', 'missing': 0})
mis.add('attribute', datamodel=dm, entalias='CAT', cname='cquantiy',  alias='c_quantity', descr='catalog quantity',
        extra={'vtype': 'uint16', 'missing': 1})
mis.add('attribute', datamodel=dm, entalias='CAT', cname='cdate',     alias='c_date',     descr='catalog date',
        extra={'vtype': 'date32', 'missing': 1})
mis.add('attribute', datamodel=dm, entalias='CAT', cname='ccheck',    alias='c_check',    descr='catalog check',
        extra={'vtype': 'bool_', 'enum': 1, 'missing': 0})

# and then view the final result on a HyperGraph
dm.get_components().to_hypergraph().out(edge_label='hasAttribute')
# . .

# oops did you see that Entity objects are not connected
# We have to add common Attribute objects that play the role of junction nodes
mis.add('attribute', datamodel=dm, entalias=['SUP', 'CAT'], cname='sid',  alias='s_ID',  descr='supplier id',
        extra={'vtype': 'uint16', 'missing': 0, 'unique': 1})
mis.add('attribute', datamodel=dm, entalias=['PRT', 'CAT'], cname='pid',  alias='p_ID',  descr='part id',
        extra={'vtype': 'uint16', 'missing': 0, 'unique': 1})

# and try to visualize again our hypergraph
dm.get_components().to_hypergraph().out(edge_label='hasAttribute')

# . .

# perfect, Entity objects share common Attribute let us verify this
# ask for the junction ONLY attributes of the DataModel
dm.get_attributes(junction=True).over('nid, parent, entities, cname, alias, descr, junction').to_dataframe().out()

# Do you remember ? EVERYTHING on Schema IS A NODE on the graph
mis.get(87)

# and because this is an Attribute, it has ONLY SchemaLinks from inward nodes
mis.get(87).in_nodes

mis.get(87).in_links

# -------------------------------------------------------------------------------------------------------------------
# clear
# -------------------------------------------------------------------------------------------------------------------
# so far so good we got you fully covered with powerful schema management what about data
# review schema
mis.overview

# get tables for MySQL dataset
mis.get(48).tables

# get all rows to a dataframe
mis.get(51).get_rows().over().to_dataframe().out()

# get 12 rows with projection to a dataframe
mis.get(51).get_rows().over('catsid, catpid, catqnt, catchk').slice(12).to_dataframe(index='catsid, catpid').out()

# get same 12 rows, from a TSV data resource
mis.get(6).get_rows().over('catsid, catpid, catqnt, catchk').slice(12).to_dataframe(index='catsid, catpid').out()

# get same 12 rows, from SQLite data resource
mis.get(28).get_rows().over('catsid, catpid, catqnt, catchk').slice(12).to_dataframe(index='catsid, catpid').out()

# get 12 rows with projection to tuples
mis.get(51).get_rows().over('catsid, catpid, catqnt, catchk').slice(12).to_tuples().out()

# get 12 rows with projection to pyarrow table with dictionary encoding and rename columns
mis.get(51).get_rows().over('catsid, catpid, catqnt, catchk', as_names='sid, pid, qnt, chk').slice(12).to_table(arrow_encoding=True).out()


# -------------------------------------------------------------------------------------------------------------------
# clear
# -------------------------------------------------------------------------------------------------------------------
# Does this end here ? No, keep watching, the best part comes...

# Add an extra data resource (Feather File DataSet) to the existing Schema,
phys_dataset = mis.add('dataset', with_components=['tables', 'fields'],
                       cname='Physicians Dataset', alias='PHYS', ctype='FEATHER',
                       descr='USA Physicians Compare National dataset',
                       extra={'path': 'Physicians'})
# Fetch Metadata
phys_dataset.get_components().to_dataframe().out()
# Fetch Table
phys_table = phys_dataset.get_tables().to_nodes().out()[0]
# Fetch Data
projection = 'npi, pacID, profID, groupID, groupTotal, last, first, gender, graduated, city, state, lbn1'
phys_table.get_rows().over(select=projection).slice(limit=10).to_table().to_dataframe().out()


# Add NEW ASET (Associative Entity Set)
# By mapping selected Field(s) from feather file onto NEW Attribute(s) of a NEW DataModel
fld_names = phys_table.get_fields().over('cname').to_tuples().out()
aliases = [f'_{num+2}' for num, _ in enumerate(fld_names)]
phys = mis.add('aset', from_table=phys_table, with_fields=fld_names,
               datamodel_name='Physician Data Model', datamodel_alias='PHYS_DM',
               entity_name='Physician', entity_alias='PHYS', as_names=aliases)
# Examine the mapping
phys.attributes


# Load data from feather file and encode them with dictionaries in a PyArrow Table
# 22 columns x 2.8 million rows on 10 years old Intel i3 core machine.
phys.dictionary_encode()

cname_list = 'npi, pacID, spec0, groupTotal, last, first, gender, graduated, city, state, lbn1, ehr, pqrs'
cond1 = 'city like ATLANTA'
cond2 = 'spec0 like ANESTHETIST'
cond3 = 'graduated>2010'
cond4 = 'gender like F'
cond5 = 'lbn1 like GRADY'
cond6 = 'pqrs like Y'

# -------------------------------------------------------------------------------------------------------------------
# clear
# -------------------------------------------------------------------------------------------------------------------
# WHAT IF YOU CAN filter in Normal Mopde
phys.reset()
phys.q.where(cond1).And(cond2).And(cond3).And(cond4).And(cond5).And(cond6).filter().count().out()

# Display result set
phys.print_rows(select=cname_list, order_by='last, first', index='npi, pacID')

# or in Associative Filtering Mode
phys.reset()
phys.select.where(cond2).And(cond4).And(cond3).And(cond6).And(cond1).And(cond5).filter().count().out()

# Display result set
phys.print_rows(select=cname_list, order_by='last, first', index='npi, pacID')

# Examine States for 'spec0' field (selected and included)
phys.spec0.print_states()

# Examine States for 'last' field (included)
phys.last.print_states()

phys.last.q.filter(phys.mask).count().out()

phys.last.q.to_array(unique=True, order='asc').out()

# -------------------------------------------------------------------------------------------------------------------
# clear
# -------------------------------------------------------------------------------------------------------------------
# WHAT IF YOU CAN display the result set on a Data Graph
phys.update_hacols_filtered_state()
values = phys.q.over('npi, last, first, groupTotal').to_string_array(unique=True).out()
hlinks = phys.q.over('npi, last, first, groupTotal').to_hyperlinks().out()

gd = GData(rebuild=True, net_name='phys_data_graph_binary', net_format='gt', net_path='/data/test/DataGraphs')
gd.add_hyperlinks(hlinks=hlinks)
gd.add_values(string_values=values)

pos = arf_layout(gd.graph)
graph_draw(gd.graph, pos, vertex_text=gd.value, vertex_font_size=12,
           vertex_shape=gd.ntype, vertex_fill_color=gd.dim2,
           vertex_pen_width=3, edge_pen_width=3, output_size=(1500, 1100))

# . .

#
# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
#
# *************************************************************************
# Now you know YOU CAN, and the limit to WHAT YOU CAN is your imagination
# *************************************************************************
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~    THE END    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

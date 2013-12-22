#include <vector>

#include "htCollWriter.h"
#include "htCollScanner.h"

htCollWriterConc::htCollWriterConc(htConnPoolPtr conn_pool,
				std::string ns,
				std::string table):
		m_conn_pool(conn_pool),
		m_table(table),
		m_stoped(false),
		m_stopping(false)
{
	htConnPool::htSession sess = m_conn_pool->get();
	m_ns = sess.client->namespace_open(ns);
}
	
void htCollWriterConc::insertAsync(KeyValue cell, std::string coll)
{
	throw "htCollWriterConc::insertAsync not implemented";
	htConnPool::htSession sess = m_conn_pool->get();
	Hypertable::ThriftGen::Future ff = sess.client->future_open(0);
	Hypertable::ThriftGen::MutatorAsync m = 
			sess.client->async_mutator_open(m_ns, m_table, ff, 0);
	
	std::vector<Hypertable::ThriftGen::Cell> cells;
	
	cells.push_back(Hypertable::ThriftGen::make_cell(cell.key.c_str(),
											coll.c_str(),
											0,
											cell.value.c_str(),
											time(0),
											0,
							Hypertable::ThriftGen::KeyFlag::INSERT));

	sess.client->async_mutator_set_cells(m, cells);
	sess.client->async_mutator_flush(m);
	sess.client->async_mutator_close(m);
	sess.client->future_close(ff);
}

void htCollWriterConc::insertSync(KeyValue cell, std::string coll)
{
	htConnPool::htSession sess = m_conn_pool->get();
	Hypertable::ThriftGen::MutateSpec mutate;
	sess.client->offer_cell(m_ns, m_table, mutate, \
			Hypertable::ThriftGen::make_cell(cell.key.c_str(),
										coll.c_str(),
										0,
										cell.value.c_str(),
										time(0),	// FIXME
										0,
						Hypertable::ThriftGen::KeyFlag::INSERT));
		
	//m_client->mutator_set_cells(m, cells);
	//m_client->mutator_close(m);
}

void htCollWriterConc::removeSync(std::string key, std::string coll)
{
	htConnPool::htSession sess = m_conn_pool->get();
	Hypertable::ThriftGen::MutateSpec mutate;
	sess.client->offer_cell(m_ns, m_table, mutate, \
			Hypertable::ThriftGen::make_cell(key.c_str(),
										coll.c_str(),
										0,
										"",
										time(0),	// FIXME
										0,
						Hypertable::ThriftGen::KeyFlag::DELETE_CELL));
}

htCollWriterConc::~htCollWriterConc()
{
//	m_client->namespace_close(m_ns);
}


/*
 if (m_mode == htCollWriter::REWRITE)
	{
		std::vector<Hypertable::ThriftGen::Cell> cells;
		Hypertable::ThriftGen::Future ff = m_client->future_open(0);
		Hypertable::ThriftGen::MutatorAsync m = 
				m_client->async_mutator_open(m_ns, m_table, ff, 0);
	
		cells.push_back(Hypertable::ThriftGen::make_cell(cell.key.c_str(),
												m_coll.c_str(),
												0,
												"",
												time(0),
												0,
								Hypertable::ThriftGen::KeyFlag::DELETE_CELL));
		
		m_client->async_mutator_set_cells(m, cells);
		m_client->async_mutator_flush(m);
		m_client->async_mutator_close(m);
		m_client->future_close(ff);
		
	}
 * 
 */
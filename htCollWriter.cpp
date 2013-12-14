#include <vector>

#include "htCollWriter.h"
#include "htCollScanner.h"

htCollWriterConc::htCollWriterConc(ThriftClientPtr client,
				std::string ns,
				std::string table):
		m_client(client),
		m_table(table),
		m_stoped(false),
		m_stopping(false)
{
	m_ns = client->namespace_open(ns);
}
	
void htCollWriterConc::insertAsync(KeyValue cell, std::string coll)
{
	throw "htCollWriterConc::insertAsync not implemented";
	Hypertable::ThriftGen::Future ff = m_client->future_open(0);
	Hypertable::ThriftGen::MutatorAsync m = 
			m_client->async_mutator_open(m_ns, m_table, ff, 0);
	
	std::vector<Hypertable::ThriftGen::Cell> cells;
	
	cells.push_back(Hypertable::ThriftGen::make_cell(cell.key.c_str(),
											coll.c_str(),
											0,
											cell.value.c_str(),
											time(0),
											0,
							Hypertable::ThriftGen::KeyFlag::INSERT));

	m_client->async_mutator_set_cells(m, cells);
	m_client->async_mutator_flush(m);
	m_client->async_mutator_close(m);
	m_client->future_close(ff);
}

void htCollWriterConc::insertSync(KeyValue cell, std::string coll)
{
	Hypertable::ThriftGen::MutateSpec mutate;
	m_client->offer_cell(m_ns, m_table, mutate, \
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
	Hypertable::ThriftGen::MutateSpec mutate;
	m_client->offer_cell(m_ns, m_table, mutate, \
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
	m_client->namespace_close(m_ns);
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
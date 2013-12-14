#include "htQuerier.h"

htQuerier::htQuerier(ThriftClientPtr client,
				std::string ns,
				std::string table):
		m_client(client),
		m_table(table)
{
	m_ns = client->namespace_open(ns);
}

std::string htQuerier::getColl(std::string key, std::string coll)
{
	
	Hypertable::ThriftGen::HqlResultAsArrays result;
	m_client->hql_query_as_arrays(result, m_ns, "select "+coll+" from "+m_table+ \
							" WHERE ROW=\'"+key+"\'");
		//	std::cout << q;
	if (result.cells.size()==0)
	{
		throw htQuerier::NO_CELLS;
	}
	if (result.cells[0].size()<4)
	{
		throw htQuerier::BROKEN_RESULT;
	}
	return result.cells[0][3];
}

htQuerier::~htQuerier()
{
	m_client->namespace_close(m_ns);
}

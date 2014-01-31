#include "htQuerier.h"
#include "htConnPool.h"

htQuerier::htQuerier(htConnPoolPtr conn_pool,
				const std::string &_ns,
				const std::string &_table):
		m_conn_pool(conn_pool),
		m_table(_table)
{
	htConnPool::htSession sess = conn_pool->get();
	m_ns = sess.client->namespace_open(_ns);
}

bool htQuerier::getColl(const std::string &_key,
							const std::string &_coll,
							std::string &_value)
{
	htConnPool::htSession sess = m_conn_pool->get();
	Hypertable::ThriftGen::HqlResultAsArrays result;
	sess.client->hql_query_as_arrays(result, m_ns, "select "+_coll+" from "+m_table+ \
							" WHERE ROW=\'"+_key+"\'");
	//	std::cout << q;
	if (result.cells.size()==0)
	{
		return false;
	}
	if (result.cells[0].size()<4)
	{
		std::cerr << "htQuerier::getColl "+_key+" BROKEN_RESULT";
		throw htQuerier::BROKEN_RESULT;
	}
	_value = result.cells[0][3];
	return true;
}

htQuerier::~htQuerier()
{
	htConnPool::htSession sess = m_conn_pool->get();
	sess.client->namespace_close(m_ns);
}

#include <queue>

#include "htCollScanner.h"
#include "htConnPool.h"

htCollScanner::htCollScanner(htConnPoolPtr conn_pool,
				std::string ns,
				std::string table,
				std::string coll,
				KeyRange _range)
{
	m_conn_pool = conn_pool;
	m_coll = coll;
	m_table = table;
	m_ns_name = ns;
	reset(_range);
}

htCollScanner::htCollScanner(const htCollScanner &a)
{
	m_conn_pool = a.m_conn_pool;
	m_coll = a.m_coll;
	m_table = a.m_table;
	m_ns = a.m_ns;
	m_ns_name = a.m_ns_name;
	reset();
}

htCollScanner::htCollScanner(const htCollScanner* a)
{
	m_conn_pool = a->m_conn_pool;
	m_coll = a->m_coll;
	m_table = a->m_table;
	m_ns = a->m_ns;
	m_ns_name = a->m_ns_name;
	reset();
}

void htCollScanner::loadMore(htConnPool::htSession &sess)
{
	std::vector<Hypertable::ThriftGen::Cell> cells;
	sess.client->next_cells(cells, m_s);
	
	if (cells.size()==0)
	{
		return;
	}
	
	for (int i = 0; i<cells.size(); i++)
		if (	cells[i].key.column_family == m_coll)
	{
			buffer.push(KeyValue(cells[i].key.row, cells[i].value));
	}
	cells.clear();
}

KeyValue htCollScanner::getNextCell()
{
	if (buffer.size()!=0)
	{
		KeyValue kv = buffer.front();
		buffer.pop();
		if (buffer.size()<10)
		{
			htConnPool::htSession sess = m_conn_pool->get();
			loadMore(sess);
		}
		return kv;
	}
	else
	{
		throw "htCollScanner::getNextCell() buffer empty";
	}
}

void htCollScanner::reset()
{
	htConnPool::htSession sess = m_conn_pool->get();
	reset(sess);
}

void htCollScanner::reset(htConnPool::htSession &sess)
{
	m_ns = sess.client->namespace_open(m_ns_name);
	m_s = sess.client->open_scanner(m_ns, m_table, m_ss);
	while (buffer.size()!=0)
		buffer.pop();
	loadMore(sess);
}

void htCollScanner::reset(const KeyRange &range)
{
	if (range.ok())
	{
		Hypertable::ThriftGen::RowInterval interval;
		interval.__isset.start_row = true;
		interval.__isset.end_row = true;
		interval.__isset.start_inclusive = true;
		interval.__isset.end_inclusive = true;
		interval.start_row = range.beg;
		interval.end_row = range.end;
		interval.start_inclusive = true;
		interval.end_inclusive = true;

		m_ss.__isset.row_intervals = true;
		std::vector<Hypertable::ThriftGen::RowInterval> intervals;
		intervals.push_back(interval);
		m_ss.__set_row_intervals(intervals);
	}
	reset();
}

bool htCollScanner::end()
{
	return buffer.size()==0;
}

htCollScanner::~htCollScanner()
{
//	m_client->scanner_close(m_s);
}


KeyValue htCollScannerConc::getNextCell()
{
	lock.lock();
	KeyValue kv = htCollScanner::getNextCell();
	lock.unlock();
	return kv;
}

bool htCollScannerConc::end()
{
	lock.lock();
	bool isend = htCollScanner::end();
	lock.unlock();
	return isend;
}

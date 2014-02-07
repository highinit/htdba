#include "htKeyScanner.h"
#include "htConnPool.h"

htKeyScanner::htKeyScanner(htConnPoolPtr conn_pool,
				std::string _ns,
				std::string table,
				const KeyRange range)
{
	m_last_key = "";
	m_table = table;
	m_conn_pool = conn_pool;
	
	m_ss.keys_only=true;
	m_ss.__isset.keys_only = true;
	
	htConnPool::htSession sess = m_conn_pool->get();
	m_ns = sess.client->namespace_open(_ns);
	
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
	
	reset(sess);
}

void htKeyScanner::reset(htConnPool::htSession sess)
{
	m_s = sess.client->open_scanner(m_ns, m_table, m_ss);
	while (buffer.size()!=0)
		buffer.pop();
	loadMore(sess);
}

void htKeyScanner::reset(const KeyRange &range)
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
	htConnPool::htSession sess = m_conn_pool->get();
	reset(sess);
}

void htKeyScanner::reset()
{
	htConnPool::htSession sess = m_conn_pool->get();
	reset(sess);
}

void htKeyScanner::loadMore(htConnPool::htSession sess)
{
	std::vector<Hypertable::ThriftGen::Cell> cells;
	//sess.client->scanner_get_cells(cells, m_s);
	try {
		sess.client->next_cells(cells, m_s);
	} catch (Hypertable::ThriftGen::ClientException &e) {
		std::cout << "thrift exception: " << e << std::endl;
	} catch (...) {
		std::cout << "thrift unknown exception ";
	}
	
	if (cells.size()==0) {
		return;
	}
	
	for (int i = 0; i<cells.size(); i++) {
		if (m_last_key != cells[i].key.row) {
			buffer.push(cells[i].key.row);
			m_last_key = cells[i].key.row;
		}
	}
	cells.clear();
}

std::string htKeyScanner::getNextKey()
{
	if (buffer.size()!=0)
	{
		std::string k = buffer.front();
		buffer.pop();
		if (buffer.size()<4096)
		{
			htConnPool::htSession sess = m_conn_pool->get();
			//for (int i = 0; i<10; i++)
			loadMore(sess);
		}
		return k;
	}
	else
	{
		
		throw std::string("htCollScanner::getNextCell() buffer empty");
	}
}

bool htKeyScanner::end()
{
	return buffer.size()==0;
}

htKeyScanner::~htKeyScanner()
{
	//m_client->scanner_close(m_s);
}

#include "htKeyScanner.h"

htKeyScanner::htKeyScanner(ThriftClientPtr client,
				std::string _ns,
				std::string table,
				const KeyRange range)
{
	m_table = table;
	m_client = client;
	
	m_ss.keys_only=true;
	m_ss.__isset.keys_only = true;
	m_ns = client->namespace_open(_ns);
	
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

void htKeyScanner::reset()
{
	m_s = m_client->open_scanner(m_ns, m_table, m_ss);
	while (buffer.size()!=0)
		buffer.pop();
	loadMore();
}

void htKeyScanner::loadMore()
{
	std::vector<Hypertable::ThriftGen::Cell> cells;
	m_client->scanner_get_cells(cells, m_s);
	
	if (cells.size()==0)
	{
		return;
	}
	
	for (int i = 0; i<cells.size(); i++)
	{
		if (i==0)
			buffer.push(cells[i].key.row);
		else if (cells[i].key.row != cells[i-1].key.row)
			buffer.push(cells[i].key.row);
	}
	cells.clear();
}

std::string htKeyScanner::getNextKey()
{
	if (buffer.size()!=0)
	{
		std::string k = buffer.front();
		buffer.pop();
		if (buffer.size()<10)
		{
			loadMore();
		}
		return k;
	}
	else
	{
		throw "htCollScanner::getNextCell() buffer empty";
	}
}

bool htKeyScanner::end()
{
	return buffer.size()==0;
}

htKeyScanner::~htKeyScanner()
{
	m_client->scanner_close(m_s);
}

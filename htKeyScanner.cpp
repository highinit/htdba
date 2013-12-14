#include "htKeyScanner.h"

htKeyScanner::htKeyScanner(ThriftClientPtr client,
				std::string _ns,
				std::string table)
{
	m_client = client;
	m_ss.keys_only=true;
	m_ss.__isset.keys_only = true;
	Hypertable::ThriftGen::Namespace ns = client->namespace_open(_ns);
	m_s = client->open_scanner(ns, table, m_ss);
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
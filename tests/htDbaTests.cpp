
#include "htDbaTests.h"
//#include "../"

htDbaTests::htDbaTests(std::string ns, std::string table):
	m_ns(ns),
	m_table(table)
{
	m_client.reset( new ThriftClient("localhost", 38080));
}

void htDbaTests::clearTable()
{
	Hypertable::ThriftGen::Namespace ns = m_client->namespace_open(m_ns);
	Hypertable::ThriftGen::HqlResult result;
	m_client->hql_query(result, ns, "drop table if exists "+m_table);
	m_client->hql_query(result, ns, \
				"create table "+m_table+" (a MAX_VERSIONS=1, b MAX_VERSIONS=1, c MAX_VERSIONS=1)");
	m_client->close_namespace(ns);
}

std::string htDbaTests::getRow(int i)
{
	char bf[50];
	sprintf(bf, "%d", i);
	return std::string(bf);
}

std::string htDbaTests::getAvalue(int row)
{
	char bf[50];
	sprintf(bf, "%d_a", row);
	return std::string(bf);
}

std::string htDbaTests::getBvalue(int row)
{
	char bf[50];
	sprintf(bf, "%d_b", row);
	return std::string(bf);
}

std::string htDbaTests::getCvalue(int row)
{
	char bf[50];
	sprintf(bf, "%d_c", row);
	return std::string(bf);
}

void htDbaTests::fill(int nrows)
{
	Hypertable::ThriftGen::Namespace ns = m_client->namespace_open(m_ns);
	
	htCollWriterConc writer(m_client, m_ns, m_table);
	
	for (int i = 0; i<nrows; i++)
	{
		writer.insertSync(KeyValue(getRow(i), getAvalue(i)), "a");
		writer.insertSync(KeyValue(getRow(i), getBvalue(i)), "b");
		writer.insertSync(KeyValue(getRow(i), getCvalue(i)), "c");
	}
	
	m_client->close_namespace(ns);
}

void htDbaTests::testCustomScanner()
{
	clearTable();
	const int nrows = 1000;
	Hypertable::ThriftGen::Namespace ns = m_client->namespace_open(m_ns);
	
	fill(nrows);
	
	sleep(15);
	
	std::set<std::string> cells;
	cells.insert("a");
	cells.insert("b");
	cells.insert("c");
	htCustomScanner cs(m_client, "test", "test", cells);
	
	int i = 0;
	while (!cs.end())
	{
		htLine cells = cs.getNextLine();
		
		/*
		
		std::cout << cells.key << " a " << cells.cells["a"] << std::endl;
		std::cout << cells.key << " b " << cells.cells["b"] << std::endl;
		std::cout << cells.key << " c " << cells.cells["c"] << std::endl;
		std::cout << "_________________________________\n";
		*/
		
		if (cells.cells["a"] != cells.key+"_a")
		{
			std::cout << "ERROR: htDbaTests::testCustomScanner got wrong value\n";
			std::cout << "key: " << cells.key << " value:" << cells.cells["a"];
			std::cout << " must be " << cells.key+"_a" << "\n";
		}
		
		if (cells.cells["b"] != cells.key+"_b")
		{
			std::cout << "ERROR: htDbaTests::testCustomScanner got wrong value\n";
			std::cout << "key: " << cells.key << " value:" << cells.cells["b"];
			std::cout << " must be " << cells.key+"_b" << "\n";
		}
		
		if (cells.cells["c"] != cells.key+"_c")
		{
			std::cout << "ERROR: htDbaTests::testCustomScanner got wrong value\n";
			std::cout << "key: " << cells.key << " value:" << cells.cells["c"];
			std::cout << " must be " << cells.key+"_c" << "\n";
		}
		
		i++;

  	}

	if (nrows != i)
	{
		std::cout << "ERROR: htDbaTests::testCustomScanner wrote: " \
				<< nrows << " got: " << i << std::endl;
	}
	
	m_client->close_namespace(ns);
}

void htDbaTests::testKeyScanner()
{
	clearTable();
	fill(10);
	sleep(2);
	
	htKeyScanner s(m_client, m_ns, m_table, KeyRange("4","7"));
	while (!s.end())
	{
		std::cout << "key:" << s.getNextKey() << std::endl;
	}
	
}

void htDbaTests::run()
{

}


#include "htDbaTests.h"
//#include "../"

htDbaTests::htDbaTests(std::string ns, std::string table):
	m_ns(ns),
	m_table(table)
{
	//m_client.reset( new ThriftClient("localhost", 38080));
	m_db_pool.reset(new htConnPool("localhost", 38080, 5));
}

void htDbaTests::clearTable()
{
	ThriftClientPtr m_client = m_db_pool->get().client;
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
	htConnPool::htSession sess = m_db_pool->get();
	Hypertable::ThriftGen::Namespace ns = sess.client->namespace_open(m_ns);
	
	htCollWriterConc writer(m_db_pool, m_ns, m_table);
	
	for (int i = 0; i<nrows; i++)
	{
		writer.insertSync(KeyValue(getRow(i), getAvalue(i)), "a");
		writer.insertSync(KeyValue(getRow(i), getBvalue(i)), "b");
		writer.insertSync(KeyValue(getRow(i), getCvalue(i)), "c");
	}
	
	sess.client->close_namespace(ns);
}

void htDbaTests::testCustomScanner()
{
	clearTable();
	ThriftClientPtr m_client = m_db_pool->get().client;
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

TaskLauncher::TaskRet htDbaTests::testKeyScannerThread(htKeyScannerPtr key_scanner)
{
	std::vector<std::string> keys;
	while (!key_scanner->end()) {
		std::string key =key_scanner->getNextKey();
		keys.push_back(key);
	}
	
	testKeyScannerLock.lock();
	for (int i = 0; i<keys.size(); i++) {
		std::cout << keys[i] << " ";
	}
	std::cout << std::endl;
	testKeyScannerLock.unlock();
	
	return TaskLauncher::NO_RELAUNCH;
}

void htDbaTests::testKeyScannerFinished()
{
	exit(0);
}

void htDbaTests::testKeyScanner()
{
	clearTable();
	fill(50);
	sleep(2);

	const int nthreads = 8;
	hThreadPool *pool = new hThreadPool(nthreads);
	TaskLauncherPtr launcher(new TaskLauncher(pool, nthreads, boost::bind(
				&htDbaTests::testKeyScannerFinished, this)));
	
	for (int i = 0; i<nthreads; i++) {
		htKeyScannerPtr key_scanner(
		new htKeyScanner(m_db_pool, m_ns, m_table ) );
		launcher->addTask(new boost::function<TaskLauncher::TaskRet()>(
				boost::bind(&htDbaTests::testKeyScannerThread, this, key_scanner) ) );
	}
	launcher->setNoMoreTasks();
	pool->run();
	pool->join();
	
	/*
	htKeyScanner s(m_db_pool, m_ns, m_table, KeyRange("4","7"));
	while (!s.end())
	{
		std::cout << "key:" << s.getNextKey() << std::endl;
	}
	*/
}

void htDbaTests::run()
{

}

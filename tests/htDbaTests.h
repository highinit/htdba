/* 
 * File:   htDbaTests.h
 * Author: phrk
 *
 * Created on December 14, 2013, 7:02 PM
 */

#ifndef HTDBATESTS_H
#define	HTDBATESTS_H

#include "../htDba.h"
#include "../htCollScanner.h"
#include "../htCollWriter.h"
#include "../htCustomScanner.h"
#include "../htQuerier.h"
#include "../htKeyScanner.h"

class htDbaTests
{
	ThriftClientPtr m_client;
	std::string m_ns;
	std::string m_table;
	
	std::string getRow(int i);
	std::string getAvalue(int row);
	std::string getBvalue(int row);
	std::string getCvalue(int row);

public:
	
	htDbaTests(std::string ns, std::string table);
	
	void clearTable();
	void fill(int rows);
	
	void testCustomScanner();
	void testKeyScanner();
	
	void run();
};

#endif	/* HTDBATESTS_H */


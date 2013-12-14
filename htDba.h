/* 
 * File:   htDba.h
 * Author: phrk
 *
 * Created on December 11, 2013, 8:15 PM
 */

#ifndef HTDBA_H
#define	HTDBA_H

#include "Common/Compat.h"
#include "Common/System.h"

#include <iostream>
#include <fstream>
#include "ThriftBroker/Client.h"
#include "ThriftBroker/gen-cpp/HqlService.h"
#include "ThriftBroker/ThriftHelper.h"
#include "ThriftBroker/SerializedCellsReader.h"

#include <string>
#include <tr1/unordered_map>

typedef Hypertable::Thrift::Client ThriftClient;
typedef boost::shared_ptr<ThriftClient> ThriftClientPtr;

class KeyValue
{
public:
	
	std::string key;
	std::string value;
	KeyValue() { } 
	KeyValue(std::string _key, std::string _value):
		key(_key),
		value(_value)
	{
	}
};

class htCell
{
public:
	
	std::string key;
	std::string coll;
	std::string value;
	
	htCell() { }
	htCell(std::string _key, std::string _coll, std::string _value):
		key(_key),
		coll(_coll),
		value(_value)
	{
	}
};

class KeyRange
{
public:
	
	std::string beg;
	std::string end;
	KeyRange() { } 
	KeyRange(std::string _beg, std::string _end):
		beg(_beg),
		end(_end)
	{
	}
};

class htLine
{
public:
	std::string key;
	std::tr1::unordered_map<std::string, std::string> cells;
};

#endif	/* HTDBA_H */


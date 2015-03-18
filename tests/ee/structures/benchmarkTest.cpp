/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include <iostream>
#include <map>
#include <algorithm>
#include <cstdlib>
#include <cstdio>
#include <sys/time.h>
#include <vector>

#include "harness.h"
#include "structures/CompactingMap.h"
#include "structures/btree_map.h"

using namespace voltdb;
using namespace std;

class StringComparator {
public:
    int comparisons;
    StringComparator() : comparisons(0) {}
    ~StringComparator() { }

    inline int operator()(const std::string &lhs, const std::string &rhs) const {
        int *comp = const_cast<int*>(&comparisons);
        *comp = comparisons + 1;
        return lhs.compare(rhs);
    }
};

class IntComparator {
public:
    inline int operator()(const int &lhs, const int &rhs) const {
        if (lhs > rhs) return 1;
        else if (lhs < rhs) return -1;
        else return 0;
    }
};

class CompactingMapBenchTest : public Test {
public:
    CompactingMapBenchTest() {
    }

    ~CompactingMapBenchTest() {
    }
};

int64_t getMicrosNow () {
    timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000 + tv.tv_usec;
}

std::vector<int> getRandomValues(int size, int max) {
    std::vector<int> vec;
    srand(getMicrosNow() % 1000000);
    for (int i = 0; i < size; i++) {
        int val = rand() % max;
        vec.push_back(val);
    }

    return vec;
}

#define VoltMap 1
#define VoltHash 2
#define STLMap 3
#define BtreeMap 4
std::string mapCategoryToString(int mapCategory) {
    switch(mapCategory) {
    case VoltMap:
        return "VoltMap";
    case VoltHash:
        return "VoltHash";
    case STLMap:
        return "STLMap";
    case BtreeMap:
        return "BtreeMap";
    default:
        return "invalid";
    }
}

class Benchmark {
public:
    Benchmark(int mapCategory) {
        m_name = mapCategory;
        m_start = 0;
        m_duration = 0;
    }

    void start() {
        printf("%s benchmark starts...", mapCategoryToString(m_name).c_str());
        m_start = getMicrosNow();
        m_duration = 0;
    }

    void stop() {
        m_duration += getMicrosNow() - m_start;
        m_start = getMicrosNow();
        printf("%s benchmark stops...", mapCategoryToString(m_name).c_str());
    }

    void print() {
        printf("%s finished in %lld microseconds\n", mapCategoryToString(m_name).c_str(), m_duration);
    }
private:
    int m_name;

    int64_t m_start;
    int64_t m_duration;
};

void resultPrinter(std::string name, int scale, std::vector<Benchmark> result) {
    printf("\n\nBenchmark: %s, scale size %d\n", name.c_str(), scale);

    for (int i = 0; i < result.size(); i++) {
        Benchmark ben = result[i];
        ben.print();
    }

    printf("\n");
}

TEST_F(CompactingMapBenchTest, RandomInsert) {
    const int NUM_OF_VALUES = 1000000;
    const int BIGGEST_VAL = 1000000;
    const int ITERATIONS_UPDATES = 10000;  // for UPDATE
    const int ITERATIONS = 100000;  // for LOOK UP and DELETE
    std::vector<Benchmark> result;

    std::vector<int> input = getRandomValues(NUM_OF_VALUES, BIGGEST_VAL);

    // INSERT
    voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator, false> voltMap(true, IntComparator());
    Benchmark benVolt(VoltMap);
    benVolt.start();
    for (int i = 0; i < NUM_OF_VALUES; i++) {
        int val = input[i];
        voltMap.insert(std::pair<int,int>(val, val));
    }
    benVolt.stop();
    result.push_back(benVolt);

    std::map<int,int> stlMap;
    Benchmark benStl(STLMap);
    benStl.start();
    for (int i = 0; i < NUM_OF_VALUES; i++) {
        int val = input[i];
        stlMap.insert(std::pair<int,int>(val, val));
    }
    benStl.stop();
    result.push_back(benStl);

    btree::btree_map<int, int, less<int>, allocator<int>, 256> btreeMap;
    Benchmark benBtree(BtreeMap);
    benBtree.start();
    for (int i = 0; i < NUM_OF_VALUES; i++) {
    	int val = input[i];
    	btreeMap.insert(std::pair<int,int>(val, val));
    }
    benBtree.stop();
    result.push_back(benBtree);

    resultPrinter("INSERT", NUM_OF_VALUES, result);
    result.clear();

    // SCAN
    voltdb::CompactingMap<NormalKeyValuePair<int, int>, IntComparator, false>::iterator iter_volt;
    iter_volt = voltMap.begin();
    benVolt.start();
    while(! iter_volt.isEnd()) {
        iter_volt.moveNext();
    }
    benVolt.stop();
    result.push_back(benVolt);

    std::map<int, int>::const_iterator iter_stl = stlMap.begin();
    benStl.start();
    while(iter_stl != stlMap.end()) {
        iter_stl++;
    }
    benStl.stop();
    result.push_back(benStl);

    btree::btree_map<int, int, less<int>, allocator<int>, 256>::iterator iter_btree = btreeMap.begin();
    benBtree.start();
    while(iter_btree != btreeMap.end()) {
    	iter_btree++;
    }
    benBtree.stop();
    result.push_back(benBtree);

    resultPrinter("SCAN", NUM_OF_VALUES, result);
    result.clear();


    // LOOK UP
    std::vector<int> keys = getRandomValues(ITERATIONS, BIGGEST_VAL);

    benVolt.start();
    for (int i = 0; i< ITERATIONS; i++) {
    	int val = keys[i];
    	iter_volt = voltMap.find(val);
    }
    benVolt.stop();
    result.push_back(benVolt);

    benStl.start();
    for (int i = 0; i< ITERATIONS; i++) {
    	int val = keys[i];
    	iter_stl = stlMap.find(val);
    }
    benStl.stop();
    result.push_back(benStl);

    benBtree.start();
    for (int i = 0; i< ITERATIONS; i++) {
    	int val = keys[i];
    	iter_btree = btreeMap.find(val);
    }
    benBtree.stop();
    result.push_back(benBtree);

    resultPrinter("LOOK UP", ITERATIONS, result);
    result.clear();


    // UPDATE
    std::vector<int> updates = getRandomValues(ITERATIONS_UPDATES, BIGGEST_VAL);

    benVolt.start();
    for (int i = 0; i< ITERATIONS_UPDATES; i++) {
    	int val = updates[i];
    	voltMap.erase(val);
    }
    benVolt.stop();
    result.push_back(benVolt);

    benStl.start();
    for (int i = 0; i< ITERATIONS_UPDATES; i++) {
    	int val = deletes[i];
    	stlMap.erase(val);
    }
    benStl.stop();
    result.push_back(benStl);

    benBtree.start();
    for (int i = 0; i< ITERATIONS_UPDATES; i++) {
    	int val = deletes[i];
    	btreeMap.erase(val);
    }
    benBtree.stop();
    result.push_back(benBtree);

    resultPrinter("UPDATE", ITERATIONS_UPDATES, result);
    result.clear();

    // DELETE
    std::vector<int> deletes = getRandomValues(ITERATIONS, BIGGEST_VAL);

    benVolt.start();
    for (int i = 0; i< ITERATIONS; i++) {
    	int val = deletes[i];
    	voltMap.erase(val);
    }
    benVolt.stop();
    result.push_back(benVolt);

    benStl.start();
    for (int i = 0; i< ITERATIONS; i++) {
    	int val = deletes[i];
    	stlMap.erase(val);
    }
    benStl.stop();
    result.push_back(benStl);

    benBtree.start();
    for (int i = 0; i< ITERATIONS; i++) {
    	int val = deletes[i];
    	btreeMap.erase(val);
    }
    benBtree.stop();
    result.push_back(benBtree);

    resultPrinter("DELETE", ITERATIONS, result);
    result.clear();
}

int main() {
    return TestSuite::globalInstance()->runAll();
}

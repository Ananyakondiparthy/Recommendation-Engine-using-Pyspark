{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# CWBTIAB  RECOMMENDATION "
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## SUBMITTED BY Sri Ananya Kondiparthy"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 97,
   "metadata": {},
   "outputs": [],
   "source": [
    "from __future__ import print_function\n",
    "import sys\n",
    "from pyspark.sql import SparkSession\n",
    "from pyspark import SparkContext\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "The spark session is initialised"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [],
   "source": [
    "spark = SparkSession.builder\\\n",
    "        .master(\"local\")\\\n",
    "        .appName(\"Amazonrec\")\\\n",
    "        .getOrCreate()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 371,
   "metadata": {},
   "outputs": [],
   "source": [
    "rdd = spark.sparkContext.textFile(\"purchases.txt\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 372,
   "metadata": {},
   "outputs": [],
   "source": [
    "#mapper_1 = rdd.map(lambda x:(x.split(\":\"))).flatMap(lambda x:[(x[0],x[1])]).distinct()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 373,
   "metadata": {},
   "outputs": [],
   "source": [
    "mapper_2 = rdd.map(lambda x:(x.split(\":\"))).flatMap(lambda x:[(x[1],x[0])])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 374,
   "metadata": {},
   "outputs": [],
   "source": [
    "rdd1 = rdd.map(lambda x: x.split(':'))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 399,
   "metadata": {},
   "outputs": [],
   "source": [
    "#list_1 = mapper_1.groupByKey().mapValues(lambda x:set(x))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 398,
   "metadata": {},
   "outputs": [],
   "source": [
    "list_2 = mapper_2.groupByKey().mapValues(lambda x:set(freq_dict(x)))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 400,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}),\n",
       " ('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'}),\n",
       " ('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'}),\n",
       " ('book4', {'u1', 'u4'}),\n",
       " ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'}),\n",
       " ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'}),\n",
       " ('book7', {'u1', 'u2'}),\n",
       " ('book0', {'u2', 'u3', 'u6'}),\n",
       " ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'}),\n",
       " ('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}),\n",
       " ('book10', {'u8'}),\n",
       " ('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       " ('book15', {'u19', 'u9'}),\n",
       " ('book16', {'u19', 'u3', 'u9'}),\n",
       " ('book13', {'u3'}),\n",
       " ('book17', {'u2'}),\n",
       " ('book19', {'u10', 'u11'}),\n",
       " ('book18', {'u10'}),\n",
       " ('book21', {'u11', 'u12'}),\n",
       " ('book22', {'u11', 'u12'}),\n",
       " ('book23', {'u12'}),\n",
       " ('book31', {'u13'}),\n",
       " ('book61', {'u13'})]"
      ]
     },
     "execution_count": 400,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "list_2.collect()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 401,
   "metadata": {},
   "outputs": [],
   "source": [
    "def freq_dict(data):\n",
    "\tfreq = {}\n",
    "\tfor elem in data:\n",
    "\t\tif elem in freq:\n",
    "\t\t\tfreq[elem] += 1\n",
    "\t\telse:\n",
    "\t\t\tfreq[elem] = 1\n",
    "\treturn freq"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 402,
   "metadata": {},
   "outputs": [],
   "source": [
    "rdd_cart = list_2.cartesian(list_2).filter(lambda x: x[0][0] < x[1][0])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 404,
   "metadata": {
    "collapsed": true
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[(('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}),\n",
       "  ('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}),\n",
       "  ('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}), ('book4', {'u1', 'u4'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}),\n",
       "  ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}),\n",
       "  ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}), ('book7', {'u1', 'u2'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}),\n",
       "  ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}),\n",
       "  ('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}), ('book10', {'u8'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}),\n",
       "  ('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}),\n",
       "  ('book15', {'u19', 'u9'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}),\n",
       "  ('book16', {'u19', 'u3', 'u9'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}), ('book13', {'u3'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}), ('book17', {'u2'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}),\n",
       "  ('book19', {'u10', 'u11'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}), ('book18', {'u10'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}),\n",
       "  ('book21', {'u11', 'u12'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}),\n",
       "  ('book22', {'u11', 'u12'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}), ('book23', {'u12'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}), ('book31', {'u13'})),\n",
       " (('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'}), ('book61', {'u13'})),\n",
       " (('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'}),\n",
       "  ('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'})),\n",
       " (('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'}),\n",
       "  ('book4', {'u1', 'u4'})),\n",
       " (('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'}),\n",
       "  ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'}),\n",
       "  ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'}),\n",
       "  ('book7', {'u1', 'u2'})),\n",
       " (('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'}), ('book4', {'u1', 'u4'})),\n",
       " (('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'}),\n",
       "  ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'}),\n",
       "  ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'}), ('book7', {'u1', 'u2'})),\n",
       " (('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'}),\n",
       "  ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'}),\n",
       "  ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'}),\n",
       "  ('book21', {'u11', 'u12'})),\n",
       " (('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'}),\n",
       "  ('book22', {'u11', 'u12'})),\n",
       " (('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'}),\n",
       "  ('book23', {'u12'})),\n",
       " (('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'}),\n",
       "  ('book31', {'u13'})),\n",
       " (('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'}),\n",
       "  ('book61', {'u13'})),\n",
       " (('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'}), ('book31', {'u13'})),\n",
       " (('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'}), ('book61', {'u13'})),\n",
       " (('book4', {'u1', 'u4'}),\n",
       "  ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book4', {'u1', 'u4'}), ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book4', {'u1', 'u4'}), ('book7', {'u1', 'u2'})),\n",
       " (('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'}),\n",
       "  ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'}),\n",
       "  ('book7', {'u1', 'u2'})),\n",
       " (('book6', {'u1', 'u13', 'u2', 'u3', 'u8'}), ('book7', {'u1', 'u2'})),\n",
       " (('book4', {'u1', 'u4'}), ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'}),\n",
       "  ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book6', {'u1', 'u13', 'u2', 'u3', 'u8'}),\n",
       "  ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book7', {'u1', 'u2'}), ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book4', {'u1', 'u4'}), ('book61', {'u13'})),\n",
       " (('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'}), ('book61', {'u13'})),\n",
       " (('book6', {'u1', 'u13', 'u2', 'u3', 'u8'}), ('book61', {'u13'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}),\n",
       "  ('book1', {'u1', 'u10', 'u12', 'u6', 'u7', 'u8'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}),\n",
       "  ('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}),\n",
       "  ('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}),\n",
       "  ('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}),\n",
       "  ('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'})),\n",
       " (('book10', {'u8'}),\n",
       "  ('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book10', {'u8'}), ('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'})),\n",
       " (('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       "  ('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       "  ('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'})),\n",
       " (('book15', {'u19', 'u9'}),\n",
       "  ('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book15', {'u19', 'u9'}),\n",
       "  ('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'})),\n",
       " (('book16', {'u19', 'u3', 'u9'}),\n",
       "  ('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book16', {'u19', 'u3', 'u9'}),\n",
       "  ('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'})),\n",
       " (('book13', {'u3'}),\n",
       "  ('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book13', {'u3'}), ('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}), ('book4', {'u1', 'u4'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}),\n",
       "  ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}), ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}), ('book7', {'u1', 'u2'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}),\n",
       "  ('book4', {'u1', 'u4'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}),\n",
       "  ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}),\n",
       "  ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}),\n",
       "  ('book7', {'u1', 'u2'})),\n",
       " (('book10', {'u8'}), ('book4', {'u1', 'u4'})),\n",
       " (('book10', {'u8'}), ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book10', {'u8'}), ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book10', {'u8'}), ('book7', {'u1', 'u2'})),\n",
       " (('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       "  ('book4', {'u1', 'u4'})),\n",
       " (('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       "  ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       "  ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       "  ('book7', {'u1', 'u2'})),\n",
       " (('book15', {'u19', 'u9'}), ('book4', {'u1', 'u4'})),\n",
       " (('book15', {'u19', 'u9'}),\n",
       "  ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book15', {'u19', 'u9'}), ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book15', {'u19', 'u9'}), ('book7', {'u1', 'u2'})),\n",
       " (('book16', {'u19', 'u3', 'u9'}), ('book4', {'u1', 'u4'})),\n",
       " (('book16', {'u19', 'u3', 'u9'}),\n",
       "  ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book16', {'u19', 'u3', 'u9'}), ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book16', {'u19', 'u3', 'u9'}), ('book7', {'u1', 'u2'})),\n",
       " (('book13', {'u3'}), ('book4', {'u1', 'u4'})),\n",
       " (('book13', {'u3'}), ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book13', {'u3'}), ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book13', {'u3'}), ('book7', {'u1', 'u2'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}),\n",
       "  ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}),\n",
       "  ('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}), ('book10', {'u8'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}),\n",
       "  ('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}), ('book15', {'u19', 'u9'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}), ('book16', {'u19', 'u3', 'u9'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}), ('book13', {'u3'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}),\n",
       "  ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}),\n",
       "  ('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}),\n",
       "  ('book15', {'u19', 'u9'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}),\n",
       "  ('book16', {'u19', 'u3', 'u9'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}), ('book13', {'u3'})),\n",
       " (('book10', {'u8'}), ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book10', {'u8'}), ('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'})),\n",
       " (('book10', {'u8'}),\n",
       "  ('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'})),\n",
       " (('book10', {'u8'}), ('book15', {'u19', 'u9'})),\n",
       " (('book10', {'u8'}), ('book16', {'u19', 'u3', 'u9'})),\n",
       " (('book10', {'u8'}), ('book13', {'u3'})),\n",
       " (('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       "  ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       "  ('book15', {'u19', 'u9'})),\n",
       " (('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       "  ('book16', {'u19', 'u3', 'u9'})),\n",
       " (('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       "  ('book13', {'u3'})),\n",
       " (('book15', {'u19', 'u9'}), ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book15', {'u19', 'u9'}), ('book16', {'u19', 'u3', 'u9'})),\n",
       " (('book16', {'u19', 'u3', 'u9'}),\n",
       "  ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book13', {'u3'}), ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book13', {'u3'}), ('book15', {'u19', 'u9'})),\n",
       " (('book13', {'u3'}), ('book16', {'u19', 'u3', 'u9'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}), ('book17', {'u2'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}), ('book19', {'u10', 'u11'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}), ('book18', {'u10'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}), ('book21', {'u11', 'u12'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}), ('book22', {'u11', 'u12'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}), ('book23', {'u12'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}), ('book31', {'u13'})),\n",
       " (('book0', {'u2', 'u3', 'u6'}), ('book61', {'u13'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}), ('book17', {'u2'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}),\n",
       "  ('book19', {'u10', 'u11'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}), ('book18', {'u10'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}),\n",
       "  ('book21', {'u11', 'u12'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}),\n",
       "  ('book22', {'u11', 'u12'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}), ('book23', {'u12'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}), ('book31', {'u13'})),\n",
       " (('book11', {'u1', 'u10', 'u11', 'u12', 'u8', 'u9'}), ('book61', {'u13'})),\n",
       " (('book10', {'u8'}), ('book17', {'u2'})),\n",
       " (('book10', {'u8'}), ('book19', {'u10', 'u11'})),\n",
       " (('book10', {'u8'}), ('book18', {'u10'})),\n",
       " (('book10', {'u8'}), ('book21', {'u11', 'u12'})),\n",
       " (('book10', {'u8'}), ('book22', {'u11', 'u12'})),\n",
       " (('book10', {'u8'}), ('book23', {'u12'})),\n",
       " (('book10', {'u8'}), ('book31', {'u13'})),\n",
       " (('book10', {'u8'}), ('book61', {'u13'})),\n",
       " (('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       "  ('book17', {'u2'})),\n",
       " (('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       "  ('book19', {'u10', 'u11'})),\n",
       " (('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       "  ('book18', {'u10'})),\n",
       " (('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       "  ('book21', {'u11', 'u12'})),\n",
       " (('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       "  ('book22', {'u11', 'u12'})),\n",
       " (('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       "  ('book23', {'u12'})),\n",
       " (('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       "  ('book31', {'u13'})),\n",
       " (('book12', {'u11', 'u12', 'u13', 'u19', 'u3', 'u8', 'u9'}),\n",
       "  ('book61', {'u13'})),\n",
       " (('book15', {'u19', 'u9'}), ('book17', {'u2'})),\n",
       " (('book15', {'u19', 'u9'}), ('book19', {'u10', 'u11'})),\n",
       " (('book15', {'u19', 'u9'}), ('book18', {'u10'})),\n",
       " (('book15', {'u19', 'u9'}), ('book21', {'u11', 'u12'})),\n",
       " (('book15', {'u19', 'u9'}), ('book22', {'u11', 'u12'})),\n",
       " (('book15', {'u19', 'u9'}), ('book23', {'u12'})),\n",
       " (('book15', {'u19', 'u9'}), ('book31', {'u13'})),\n",
       " (('book15', {'u19', 'u9'}), ('book61', {'u13'})),\n",
       " (('book16', {'u19', 'u3', 'u9'}), ('book17', {'u2'})),\n",
       " (('book16', {'u19', 'u3', 'u9'}), ('book19', {'u10', 'u11'})),\n",
       " (('book16', {'u19', 'u3', 'u9'}), ('book18', {'u10'})),\n",
       " (('book16', {'u19', 'u3', 'u9'}), ('book21', {'u11', 'u12'})),\n",
       " (('book16', {'u19', 'u3', 'u9'}), ('book22', {'u11', 'u12'})),\n",
       " (('book16', {'u19', 'u3', 'u9'}), ('book23', {'u12'})),\n",
       " (('book16', {'u19', 'u3', 'u9'}), ('book31', {'u13'})),\n",
       " (('book16', {'u19', 'u3', 'u9'}), ('book61', {'u13'})),\n",
       " (('book13', {'u3'}), ('book17', {'u2'})),\n",
       " (('book13', {'u3'}), ('book19', {'u10', 'u11'})),\n",
       " (('book13', {'u3'}), ('book18', {'u10'})),\n",
       " (('book13', {'u3'}), ('book21', {'u11', 'u12'})),\n",
       " (('book13', {'u3'}), ('book22', {'u11', 'u12'})),\n",
       " (('book13', {'u3'}), ('book23', {'u12'})),\n",
       " (('book13', {'u3'}), ('book31', {'u13'})),\n",
       " (('book13', {'u3'}), ('book61', {'u13'})),\n",
       " (('book17', {'u2'}),\n",
       "  ('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book17', {'u2'}), ('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'})),\n",
       " (('book19', {'u10', 'u11'}),\n",
       "  ('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book19', {'u10', 'u11'}),\n",
       "  ('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'})),\n",
       " (('book18', {'u10'}),\n",
       "  ('book2', {'u1', 'u12', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book18', {'u10'}), ('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'})),\n",
       " (('book21', {'u11', 'u12'}),\n",
       "  ('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'})),\n",
       " (('book22', {'u11', 'u12'}),\n",
       "  ('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'})),\n",
       " (('book23', {'u12'}), ('book3', {'u1', 'u12', 'u13', 'u5', 'u6', 'u7'})),\n",
       " (('book17', {'u2'}), ('book4', {'u1', 'u4'})),\n",
       " (('book17', {'u2'}), ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book17', {'u2'}), ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book17', {'u2'}), ('book7', {'u1', 'u2'})),\n",
       " (('book19', {'u10', 'u11'}), ('book4', {'u1', 'u4'})),\n",
       " (('book19', {'u10', 'u11'}),\n",
       "  ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book19', {'u10', 'u11'}), ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book19', {'u10', 'u11'}), ('book7', {'u1', 'u2'})),\n",
       " (('book18', {'u10'}), ('book4', {'u1', 'u4'})),\n",
       " (('book18', {'u10'}), ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book18', {'u10'}), ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book18', {'u10'}), ('book7', {'u1', 'u2'})),\n",
       " (('book21', {'u11', 'u12'}), ('book4', {'u1', 'u4'})),\n",
       " (('book21', {'u11', 'u12'}),\n",
       "  ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book21', {'u11', 'u12'}), ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book21', {'u11', 'u12'}), ('book7', {'u1', 'u2'})),\n",
       " (('book22', {'u11', 'u12'}), ('book4', {'u1', 'u4'})),\n",
       " (('book22', {'u11', 'u12'}),\n",
       "  ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book22', {'u11', 'u12'}), ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book22', {'u11', 'u12'}), ('book7', {'u1', 'u2'})),\n",
       " (('book23', {'u12'}), ('book4', {'u1', 'u4'})),\n",
       " (('book23', {'u12'}), ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book23', {'u12'}), ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book23', {'u12'}), ('book7', {'u1', 'u2'})),\n",
       " (('book31', {'u13'}), ('book4', {'u1', 'u4'})),\n",
       " (('book31', {'u13'}), ('book5', {'u1', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'})),\n",
       " (('book31', {'u13'}), ('book6', {'u1', 'u13', 'u2', 'u3', 'u8'})),\n",
       " (('book31', {'u13'}), ('book7', {'u1', 'u2'})),\n",
       " (('book61', {'u13'}), ('book7', {'u1', 'u2'})),\n",
       " (('book17', {'u2'}), ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book19', {'u10', 'u11'}), ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book18', {'u10'}), ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book21', {'u11', 'u12'}), ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book22', {'u11', 'u12'}), ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book23', {'u12'}), ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book31', {'u13'}), ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book61', {'u13'}), ('book9', {'u1', 'u2', 'u3', 'u4', 'u5', 'u6'})),\n",
       " (('book17', {'u2'}), ('book19', {'u10', 'u11'})),\n",
       " (('book17', {'u2'}), ('book18', {'u10'})),\n",
       " (('book17', {'u2'}), ('book21', {'u11', 'u12'})),\n",
       " (('book17', {'u2'}), ('book22', {'u11', 'u12'})),\n",
       " (('book17', {'u2'}), ('book23', {'u12'})),\n",
       " (('book17', {'u2'}), ('book31', {'u13'})),\n",
       " (('book17', {'u2'}), ('book61', {'u13'})),\n",
       " (('book19', {'u10', 'u11'}), ('book21', {'u11', 'u12'})),\n",
       " (('book19', {'u10', 'u11'}), ('book22', {'u11', 'u12'})),\n",
       " (('book19', {'u10', 'u11'}), ('book23', {'u12'})),\n",
       " (('book19', {'u10', 'u11'}), ('book31', {'u13'})),\n",
       " (('book19', {'u10', 'u11'}), ('book61', {'u13'})),\n",
       " (('book18', {'u10'}), ('book19', {'u10', 'u11'})),\n",
       " (('book18', {'u10'}), ('book21', {'u11', 'u12'})),\n",
       " (('book18', {'u10'}), ('book22', {'u11', 'u12'})),\n",
       " (('book18', {'u10'}), ('book23', {'u12'})),\n",
       " (('book18', {'u10'}), ('book31', {'u13'})),\n",
       " (('book18', {'u10'}), ('book61', {'u13'})),\n",
       " (('book21', {'u11', 'u12'}), ('book22', {'u11', 'u12'})),\n",
       " (('book21', {'u11', 'u12'}), ('book23', {'u12'})),\n",
       " (('book21', {'u11', 'u12'}), ('book31', {'u13'})),\n",
       " (('book21', {'u11', 'u12'}), ('book61', {'u13'})),\n",
       " (('book22', {'u11', 'u12'}), ('book23', {'u12'})),\n",
       " (('book22', {'u11', 'u12'}), ('book31', {'u13'})),\n",
       " (('book22', {'u11', 'u12'}), ('book61', {'u13'})),\n",
       " (('book23', {'u12'}), ('book31', {'u13'})),\n",
       " (('book23', {'u12'}), ('book61', {'u13'})),\n",
       " (('book31', {'u13'}), ('book61', {'u13'}))]"
      ]
     },
     "execution_count": 404,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "rdd_cart.collect()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 382,
   "metadata": {},
   "outputs": [],
   "source": [
    "import math"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 433,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "14"
      ]
     },
     "execution_count": 433,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "freq = rdd1.map(lambda x: (x[0])).distinct().count()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 434,
   "metadata": {},
   "outputs": [],
   "source": [
    "def sparsematrix(inp,freq):\n",
    "    if inp == 0:\n",
    "        return 0\n",
    "    else:\n",
    "        book1,book2 = inp[0],inp[1]\n",
    "        book1_name,book2_name,book1_user,book2_user = book1[0],book2[0],book1[1],book2[1]\n",
    "        both_books,just_book1,just_book2,neither_books = len(book1_user.intersection(book2_user)),len(book1_user - book2_user),len(book2_user - book1_user), freq - len(book1_user.union(book2_user))\n",
    "        phi = ((both_books * neither_books) - (just_book1 * just_book2))/ math.sqrt((both_books + just_book1)*(just_book2+neither_books)*(both_books + just_book2)*(just_book1 + neither_books))\n",
    "    return(book1_name,book2_name,phi)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 449,
   "metadata": {},
   "outputs": [],
   "source": [
    "phi_rdd = rdd_cart.map(lambda x :sparsematrix(x,freq))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 450,
   "metadata": {},
   "outputs": [],
   "source": [
    "corr_rdd = phi_rdd.map(lambda x: (x[0],(x[1],x[2]))).sortBy(lambda x: (x[1][1]), ascending= False).groupByKey().mapValues(lambda x: list(x))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 451,
   "metadata": {},
   "outputs": [],
   "source": [
    "recomm_rdd = corr_rdd.map(lambda x: (x[0],x[1][0:2]))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 462,
   "metadata": {
    "collapsed": true
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[('book21', [('book22', 1.0), ('book23', 0.6793662204867574)]),\n",
       " ('book31', [('book61', 1.0), ('book6', 0.3721042037676254)]),\n",
       " ('book15',\n",
       "  [('book16', 0.7817359599705717), ('book17', -0.11322770341445956)]),\n",
       " ('book2', [('book5', 0.7453559924999299), ('book9', 0.6454972243679028)]),\n",
       " ('book17', [('book7', 0.6793662204867574), ('book6', 0.3721042037676254)]),\n",
       " ('book18',\n",
       "  [('book19', 0.6793662204867574), ('book23', -0.07692307692307693)]),\n",
       " ('book22', [('book23', 0.6793662204867574), ('book3', 0.05892556509887897)]),\n",
       " ('book0', [('book9', 0.6030226891555273), ('book13', 0.5310850045437943)]),\n",
       " ('book5', [('book9', 0.5773502691896257), ('book6', 0.14907119849998599)]),\n",
       " ('book6', [('book7', 0.5477225575051662), ('book61', 0.3721042037676254)]),\n",
       " ('book13', [('book16', 0.5310850045437943), ('book6', 0.3721042037676254)]),\n",
       " ('book12', [('book16', 0.5222329678670935), ('book15', 0.40824829046386296)]),\n",
       " ('book4', [('book9', 0.47140452079103173), ('book7', 0.4166666666666667)]),\n",
       " ('book7', [('book9', 0.47140452079103173)]),\n",
       " ('book11',\n",
       "  [('book19', 0.47140452079103173), ('book21', 0.47140452079103173)]),\n",
       " ('book1', [('book3', 0.4166666666666667), ('book11', 0.4166666666666667)]),\n",
       " ('book19', [('book21', 0.4166666666666667), ('book22', 0.4166666666666667)]),\n",
       " ('book10', [('book6', 0.3721042037676254), ('book11', 0.32025630761017426)]),\n",
       " ('book3', [('book31', 0.32025630761017426), ('book61', 0.32025630761017426)]),\n",
       " ('book23',\n",
       "  [('book3', 0.32025630761017426), ('book31', -0.07692307692307693)]),\n",
       " ('book16',\n",
       "  [('book6', -0.025949964805384102), ('book9', -0.10050378152592121)]),\n",
       " ('book61',\n",
       "  [('book7', -0.11322770341445956), ('book9', -0.24019223070763068)])]"
      ]
     },
     "execution_count": 462,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "recomm_rdd.collect()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 479,
   "metadata": {},
   "outputs": [],
   "source": [
    "def recommendation(x):\n",
    "    if len(x[1]) == 2:\n",
    "        return(x[0],x[1][0][0], x[1][1][0])\n",
    "    else:\n",
    "        return(x[0],x[1][0][0])\n",
    "    "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 492,
   "metadata": {},
   "outputs": [],
   "source": [
    "part2  = recomm_rdd.map(lambda x: list(recommendation(x)))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 497,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[['book21', 'book22', 'book23'],\n",
       " ['book31', 'book61', 'book6'],\n",
       " ['book15', 'book16', 'book17'],\n",
       " ['book2', 'book5', 'book9'],\n",
       " ['book17', 'book7', 'book6'],\n",
       " ['book18', 'book19', 'book23'],\n",
       " ['book22', 'book23', 'book3'],\n",
       " ['book0', 'book9', 'book13'],\n",
       " ['book5', 'book9', 'book6'],\n",
       " ['book6', 'book7', 'book61'],\n",
       " ['book13', 'book16', 'book6'],\n",
       " ['book12', 'book16', 'book15'],\n",
       " ['book4', 'book9', 'book7'],\n",
       " ['book7', 'book9'],\n",
       " ['book11', 'book19', 'book21'],\n",
       " ['book1', 'book3', 'book11'],\n",
       " ['book19', 'book21', 'book22'],\n",
       " ['book10', 'book6', 'book11'],\n",
       " ['book3', 'book31', 'book61'],\n",
       " ['book23', 'book3', 'book31'],\n",
       " ['book16', 'book6', 'book9'],\n",
       " ['book61', 'book7', 'book9']]"
      ]
     },
     "execution_count": 497,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    " part2.collect()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.6"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}

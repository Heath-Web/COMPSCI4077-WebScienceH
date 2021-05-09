# COMPSCI4077-WebScienceH
2021 COMPSCI4077 web science coursework (Level H)

This porject is a Twitter crawler including Streaming API clawler and hybrid architecture (Streaming + REST API )crawler. Single pass clustering algorithm with cosine similarity was implemented here for grouping twitters collected from Streaming API. The content/topic that hybrid architecture crawler mainly forcus on is based on the growth speed of the cluster in a specific time period, including hashtags, user mentions, geo location and terms.

The repository consists of 4 folders:

- Report
- Code
- Sample data 
- Multimedia

The _Code_ folder contains the python files implemented to carry out all tasks required. There are two Json format data sets in _Sample data and multimedia_ folder, which is collected by Streaming Crawler and Hybrid Architecture Crawler respectively. The _Multimedia_ folder involves the multimedia downloaded in Streaming Crawler. 

In order to be able to run the code:

1. `cd Code`
2. Install dependencies by running: `pip install -r requirements.txt`
3. To run Streaming API Crawler run: `python StreamCrawler.py`
4. To run Data Grouping run : `python DataGrouping.py`
5. To run Hybrid Architecture Crawler run: `python HybridCrawler.py`

Twitter Crawler
Usage:
1. Put your Access token, secret, .... to config.txt
2. ./search.py 'pattern_to_search' 'file_to_write'
3. If the proccess is killed or exception is raised during collecting data. Use $tail 'output_file' for retrieving the last ID of tweets are being collected. Goto 4. Else Goto 5
4. ./search_maxid.py 'pattern_to_search' 'file_to_write' maxID_above

5.Count the tweets by date
$./counting.sh 'tweets_file_name'
Make sure to modify the array of dates in counting.sh before running it



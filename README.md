# Access pattern engine

## Purpose
Project demonstrates generation of statistics from apache user access logs using apache spark batch processing

## Generated statistics
1. Visited URLs with count

Scans all URLs from access log file and prepares URL vs count map for all visitors. Using this we can derive statistics of most frequently visited URLs etc.

2. IPs with security breach history (tried to access /admin url more than 5 times)

Scans logs to detect `403` error for `/admin` page. If any user has visited `/admin` page without proper access rights (ie. `403`) more than `5` times,
code will put those IPs in blacklist

3. Average response time

Scans access log to see average response time of website for all users, all URLs. This is done by computing average `mean` of response time of all URLs visited till now.
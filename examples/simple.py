from anadama import RunContext

ctx = RunContext()
ctx.do("wget -qO- checkip.dyndns.com > @{my_ip.txt}")
ctx.do(r"sed 's|.*Address: \(.*[0-9]\)<.*|\1|' #{my_ip.txt} > @{ip.txt}")
ctx.do("whois $(cat #{ip.txt}) > @{whois.txt}")
ctx.go()

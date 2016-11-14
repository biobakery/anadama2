# -*- coding: utf-8 -*-
from anadama2 import Workflow

ctx = Workflow(remove_options=["input","output"])
ctx.do("wget -qO- checkip.dyndns.com > [t:my_ip.txt]")
ctx.do(r"sed 's|.*Address: \(.*[0-9]\)<.*|\1|' [d:my_ip.txt] > [t:ip.txt]")
ctx.do("whois $(cat [d:ip.txt]) > [t:whois.txt]")
ctx.go()

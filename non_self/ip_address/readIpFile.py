#!/usr/bin/python
#coding:utf8

from struct import *
from addressDb import *
import string
import re


def ip2string(ip):
    a = (ip & 0xff000000) >> 24
    b = (ip & 0x00ff0000) >> 16
    c = (ip & 0x0000ff00) >> 8
    d = ip & 0x000000ff
    return "%d.%d.%d.%d" % (a,b,c,d)
 

def string2ip(str):
    ss = string.split(str, '.')
    ip = 0L
    for s in ss:
        ip = (ip << 8) + string.atoi(s)
    return ip

 
class IpLocater:
    def __init__(self, ipdb_file):
        self.ipdb = open(ipdb_file, "rb")
        # get index address 
        str = self.ipdb.read(8)
        (self.first_index,self.last_index) = unpack('II',str)
        self.index_count = (self.last_index - self.first_index) / 7 + 1

   
        # 地址库
        self.addressDb = AddressDb()

        # 从行政区.txt读取的国内行政区域映射表
        self.area_map = self.addressDb.area_map
        self.reverse_area_map = self.addressDb.reverse_area_map

        # 洲际区域划分
        self.areaMap = self.addressDb.areaMap

        # 取国家与区域对应关系
        self.countryMap = self.addressDb.countryMap

        # 国家中英文名称、与首都，暂时不使用
        self.countrySet = self.addressDb.countrySet
        self.countryList = self.addressDb.countryList

        # 大学列表
        self.universityMap = self.addressDb.universityMap

        # 国内省份列表
        self.provinceMap = self.addressDb.provinceMap

        # 带经纬度的行政区编码
        self.area_code_position_map = self.addressDb.area_code_position_map
        self.area_name_position_map = self.addressDb.area_code_position_map



    def getString(self, offset=0):
        if offset:
            self.ipdb.seek(offset)
        str = ""
        ch = self.ipdb.read(1)
        (byte,) = unpack('B',ch)
        while byte != 0:
            str = str + ch
            ch = self.ipdb.read(1)
            (byte,) = unpack('B',ch) 
        return str
 

    def getLong3(self, offset=0):
        if offset:
            self.ipdb.seek(offset)
        str = self.ipdb.read(3)
        (a,b) = unpack('HB',str)
        return (b << 16) + a
 

    def getAreaAddr(self, offset=0):
        if offset:
            self.ipdb.seek(offset)
        str = self.ipdb.read(1)
        (byte,) = unpack('B',str)
        # print byte
        if byte == 0x01 or byte == 0x02:
            p = self.getLong3()
            if p:
                return self.getString(p)
            else:
                return ""
        else:
            self.ipdb.seek(-1,1)
            # print self.getString(offset)
            return self.getString(offset)
 

    def getAddr(self, offset, ip=0):
        self.ipdb.seek(offset)
        str = self.ipdb.read(4)
        (ip,) = unpack("I",str)
        #print "%d" % ip
        endIp = ip2string(ip)
        
        #self.ipdb.seek(offset + 4)
 
        countryAddr = ""
        areaAddr = ""
        str = self.ipdb.read(1)
        (byte,) = unpack('B',str)
        if byte == 0x01:
            countryOffset = self.getLong3()
            self.ipdb.seek(countryOffset)
            str = self.ipdb.read(1)
            (b,) = unpack('B',str)
            if b == 0x02:
                countryAddr = self.getString(self.getLong3())
                self.ipdb.seek(countryOffset + 4)
            else:
                countryAddr = self.getString(countryOffset)
            areaAddr = self.getAreaAddr()
            # print byte, b, areaAddr
        elif byte == 0x02:
            countryAddr = self.getString(self.getLong3())
            areaAddr = self.getAreaAddr(offset + 8)
        else:
            countryAddr = self.getString(offset + 4)
            areaAddr = self.getAreaAddr()
        
        return (endIp, countryAddr, areaAddr)
             

    def find(self, ip, left, right):
        if right - left == 1:
            return left
        else:
            middle = (left + right) / 2
            offset = self.first_index + middle * 7
            self.ipdb.seek(offset)
            buf = self.ipdb.read(4)
            (new_ip,) = unpack("I",buf)
            if ip <= new_ip:
                return self.find(ip, left, middle)
            else:
                return self.find(ip, middle, right)
 
    def getIpAddr(self, ip):
        index = self.find(ip, 0, self.index_count - 1)
        ioffset = self.first_index + index * 7
        aoffset = self.getLong3(ioffset + 4)
        (endIp, address, areaAddr) = self.getAddr(aoffset)
        # print endIp, address, areaAddr
        return address  


    def output_cn_flat(self, first, last):
        if last > self.index_count:
            last = self.index_count

        cn_ip_count = 0
        for index in range(first,last):
            offset = self.first_index + index * 7
            self.ipdb.seek(offset)
            buf = self.ipdb.read(7)
            (ip,of1,of2) = unpack("IHB", buf)
            startIp = ip2string(ip)
            (endIp, address, areaAddr) = self.getAddr(of1 + (of2 << 16)) 
            numStartIp = string2ip(startIp)
            numEndIp = string2ip(endIp)
            # print "%s, %s, %s" % (startIp, endIp, address[:6])

            address = address.strip()
            areaAddr = areaAddr.strip()

            # 按国家名来存
            address_utf8 = address.decode('gbk').encode('utf8')
            try:
                areaAddr_utf8 = areaAddr.decode('gbk').encode('utf8')
            except UnicodeDecodeError, e:
                # print areaAddr, type(areaAddr)
                areaAddr_utf8 = '地址字符编码错误'
            
            if address_utf8 in self.countryMap.keys():
                continue

            for key in ('大学', '学院'):
                universityIndex = address_utf8.find(key)
                if universityIndex > 0:
                    universityName = address_utf8[:universityIndex] + key
                    address_utf8 = universityName

            if self.universityMap.has_key(address_utf8):
                address_utf8 = self.universityMap[address_utf8]
                city_utf8 = address_utf8[:6]
            else:
                city = address[:4]
                city_utf8 = city.decode('gbk').encode('utf8')

            if self.provinceMap.has_key(city_utf8):
                # print address_utf8
                areaCode, findAddress = self.addressDb.getAreaCode(address_utf8)
                # print (('[%s],%s,%s') % (address_utf8, findAddress, areaCode)).decode('utf8').encode('gbk')
                # print areaCode, address.decode('utf8').encode('gbk')

                provinceCode = self.provinceMap.get(city_utf8)
                (lon, lat) = self.addressDb.getLonLatFromAreaCode(areaCode)
                for numIp in range(numStartIp, numEndIp):
                    line = '|@|'.join([ip2string(numIp), str(numIp), address_utf8, areaCode, findAddress, lon, lat])
                    print line


    def output(self, first, last):
        ret = []
        if last > self.index_count:
            last = self.index_count

        cn_ip_count = 0
        for index in range(first,last):
            offset = self.first_index + index * 7
            self.ipdb.seek(offset)
            buf = self.ipdb.read(7)
            (ip,of1,of2) = unpack("IHB", buf)
            startIp = ip2string(ip)
            (endIp, address, areaAddr) = self.getAddr(of1 + (of2 << 16)) 
            numStartIp = string2ip(startIp)
            numEndIp = string2ip(endIp)

            address = address.strip()
            areaAddr = areaAddr.strip()

            # 按国家名来存
            address_utf8 = address.decode('gbk').encode('utf8')
            try:
                areaAddr_utf8 = areaAddr.decode('gbk').encode('utf8')
            except UnicodeDecodeError, e:
                # print areaAddr, type(areaAddr)
                areaAddr_utf8 = '地址字符编码错误'
            
            if address_utf8 in self.countryMap.keys():
                (area, continentName) = self.countryMap[address_utf8]
                line = '\001'.join([startIp, endIp, str(numStartIp), str(numEndIp), continentName, area, address_utf8, '', '', '', '', ''])
                # print line
                out = '\001'.join([continentName, area, address_utf8, '', '', '', '', ''])
                ret.append((startIp, endIp, str(numStartIp), str(numEndIp), out))
                continue

            for key in ('大学', '学院'):
                universityIndex = address_utf8.find(key)
                if universityIndex > 0:
                    universityName = address_utf8[:universityIndex] + key
                    address_utf8 = universityName

            if self.universityMap.has_key(address_utf8):
                address_utf8 = self.universityMap[address_utf8]
                city_utf8 = address_utf8[:6]
            else:
                city = address[:4]
                city_utf8 = city.decode('gbk').encode('utf8')

            if self.provinceMap.has_key(city_utf8):
                # print address_utf8
                areaCode, findAddress = self.addressDb.getAreaCode(address_utf8)
                # print (('[%s],%s,%s') % (address_utf8, findAddress, areaCode)).decode('utf8').encode('gbk')
                # print areaCode, address.decode('utf8').encode('gbk')

                provinceCode = self.provinceMap.get(city_utf8)
                (lon, lat) = self.addressDb.getLonLatFromAreaCode(areaCode)
                line = '\001'.join([startIp, endIp, str(numStartIp), str(numEndIp), '亚洲', '东亚', '中国', address_utf8, areaCode, findAddress, lon, lat])
                # cn_ip_count += numEndIp - numStartIp
                # print line
                out = '\001'.join(['亚洲', '东亚', '中国', address_utf8, areaCode, findAddress, lon, lat])
                ret.append((startIp, endIp, str(numStartIp), str(numEndIp), out))
            else:
                line = '\001'.join([startIp, endIp, str(numStartIp), str(numEndIp), '未知区域', '其它', '未知', address_utf8, '', '', '', ''])
                # print line
                out = '\001'.join(['未知区域', '其它', '未知', address_utf8, '', '', '', ''])
                ret.append((startIp, endIp, str(numStartIp), str(numEndIp), out))
                # print '[%s][%s][%s]' % (address, areaAddr, startIp)
                # print startIp
                # print "%s, %s, %s, %s, %s" % (startIp, endIp, address[:6], address, areaAddr)
        # print cn_ip_count
        return ret


    def distinct(self, ip_list):
        """IP地址列表去重，将相邻IP段，地址相同的记录合并"""
        i = 0
        while i < len(ip_list):
            (pre_str_start_ip, pre_str_end_ip, pre_num_start_ip, pre_num_end_ip, pre_out) = ip_list[i]

            while i < len(ip_list):
                i += 1
                if i >= len(ip_list) - 1: 
                    break
                (str_start_ip, str_end_ip, num_start_ip, num_end_ip, out) = ip_list[i]
                if pre_out == out and string.atoi(pre_num_end_ip) + 1 == string.atoi(num_start_ip):
                    pre_str_end_ip = str_end_ip
                    pre_num_end_ip = num_end_ip
                else:
                    break

            print '\001'.join([pre_str_start_ip, pre_str_end_ip, pre_num_start_ip, pre_num_end_ip, pre_out])




if __name__ == "__main__":
    ip_locater = IpLocater("qqwry.dat")

    # print string2ip('27.123.16.5')

    # ip_locater.output(1, 110)
    # ip_list = ip_locater.output(1, 10000)
    ip_list = ip_locater.output(1, ip_locater.index_count)
    # print len(ip_list)
    ip_locater.distinct(ip_list)
    # ip_locater.output_cn_flat(1, 5)
    # for province in provinceMap.keys():
    #     # print province
    #     pass
    #ip_locater.output(1, ip_locater.index_count)
    # ip = '59.64.234.174'
    # # ip = '1.2.0.0'
    # address = ip_locater.getIpAddr(string2ip(ip))
    # print "the ip %s come from %s" % (ip,address)
    
    # ip = '128.64.96.229'
    # address = ip_locater.getIpAddr(string2ip(ip))
    # print "the ip %s come from %s" % (ip,address)
    #print "Count:%d" % (ip_locater.index_count)
    # print ("四川省成都市"[:6]).decode('utf8').encode('gbk')
    # print string2ip('127.0.0.1')
    # print string2ip('192.168.0.1')
    # print string2ip('10.152.6.1')
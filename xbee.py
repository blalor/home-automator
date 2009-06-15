# By Amit Snyderman <amit@amitsnyderman.com>
# $Id$

import array

class xbee(object):
    
    START_IOPACKET   = 0x7e
    SERIES1_IOPACKET = 0x83
    
    def find_packet(serial):
        if ord(serial.read()) == xbee.START_IOPACKET:
            lengthMSB = ord(serial.read())
            lengthLSB = ord(serial.read())
            length = (lengthLSB + (lengthMSB << 8)) + 1
            return serial.read(length)
        else:
            return None
    
    
    find_packet = staticmethod(find_packet)
    
    def __init__(self, arg):
        self.digital_samples = []
        self.analog_samples = []
        self.app_id = 0
        self.address_16 = None
        self.address_64 = None
        self.rssi = None
        self.address_broadcast = None
        self.pan_broadcast = None
        self.checksum = None
        
        self.init_with_packet(arg)
    
    
    def init_with_packet(self, p):
        p = [ord(c) for c in p]
        # print [hex(c) for c in p]
        self.app_id = p[0]
        
        if self.app_id == 0x80: # RX (Receive) Packet: 64-bit Address
            # 800013a2004053a365550030208a
            # ['0x80', '0x0', '0x13', '0xa2', '0x0', '0x40', '0x53', '0xa3', '0x65', '0x55', '0x0', '0x30', '0x20', '0x8a']
            
            self.address_64 = ''.join(['%0.2X' % (c,) for c in p[1:9]])
            
            self.rssi = p[9]
            self.address_broadcast = ((p[10] >> 1) & 0x01) == 1
            self.pan_broadcast = ((p[10] >> 2) & 0x01) == 1
            
            self.data = p[11:-1]
            self.checksum = p[-1]
        
        elif self.app_id == 0x81: # RX (Receive) Packet: 16-bit Address
            self.address_16 = ''.join(['%0.2X' % (c,) for c in p[1:3]])

            self.rssi = p[3]
            self.address_broadcast = ((p[4] >> 1) & 0x01) == 1
            self.pan_broadcast = ((p[4] >> 2) & 0x01) == 1

            self.data = p[5:-1]
            self.checksum = p[-1]
        
        ## elif self.app_id == xbee.SERIES1_IOPACKET:
        ##     addrMSB = p[1]
        ##     addrLSB = p[2]
        ##     self.address_16 = (addrMSB << 8) + addrLSB
        ##     
        ##     self.rssi = p[3]
        ##     self.address_broadcast = ((p[4] >> 1) & 0x01) == 1
        ##     self.pan_broadcast = ((p[4] >> 2) & 0x01) == 1
        ##     
        ##     self.total_samples = p[5]
        ##     self.channel_indicator_high = p[6]
        ##     self.channel_indicator_low = p[7]
        ##     
        ##     local_checksum = int(self.app_id, 16) + addrMSB + addrLSB + self.rssi + p[4] + self.total_samples + self.channel_indicator_high + self.channel_indicator_low
        ##     
        ##     for n in range(self.total_samples):
        ##         dataD = [-1] * 9
        ##         digital_channels = self.channel_indicator_low
        ##         digital = 0
        ##         
        ##         for i in range(len(dataD)):
        ##             if (digital_channels & 1) == 1:
        ##                 dataD[i] = 0
        ##                 digital = 1
        ##             digital_channels = digital_channels >> 1
        ##         
        ##         if (self.channel_indicator_high & 1) == 1:
        ##             dataD[8] = 0
        ##             digital = 1
        ##         
        ##         if digital:
        ##             digMSB = p[8]
        ##             digLSB = p[9]
        ##             local_checksum += digMSB + digLSB
        ##             dig = (digMSB << 8) + digLSB
        ##             for i in range(len(dataD)):
        ##                 if dataD[i] == 0:
        ##                     dataD[i] = dig & 1
        ##                 dig = dig >> 1
        ##         
        ##         self.digital_samples.append(dataD)
        ##         
        ##         analog_count = None
        ##         dataADC = [-1] * 6
        ##         analog_channels = self.channel_indicator_high >> 1
        ##         for i in range(len(dataADC)):
        ##             if (analog_channels & 1) == 1:
        ##                 dataADCMSB = p[9 + i * n]
        ##                 dataADCLSB = p[10 + i * n]
        ##                 local_checksum += dataADCMSB + dataADCLSB
        ##                 dataADC[i] = ((dataADCMSB << 8) + dataADCLSB) / 64
        ##                 analog_count = i
        ##             analog_channels = analog_channels >> 1
        ##         
        ##         self.analog_samples.append(dataADC)
        ##         
        ##     checksum = p[10 + analog_count * n]
        ##     local_checksum = 0xff - local_checksum;
        ##     
        ##     # if (checksum - local_checksum != 0):
        ##     #   print "Checksum error! checksum: %s, local_checksum: %s" % (checksum, local_checksum)
    
    def __str__(self):
        # return "<xbee {app_id: %s, address_16: %s, rssi: %s, address_broadcast: %s, pan_broadcast: %s, total_samples: %s, digital: %s, analog: %s}>" % (self.app_id, self.address_16, self.rssi, self.address_broadcast, self.pan_broadcast, self.total_samples, self.digital_samples, self.analog_samples)
        return "<xbee {app_id: %s, address_16: %s, address_64: %s, rssi: %s, address_broadcast: %s, pan_broadcast: %s}>" % (self.app_id, self.address_16, self.address_64, self.rssi, self.address_broadcast, self.pan_broadcast)

http://www.digi.com/wiki/developer/index.php/XBee_Product_Codes

{'command': 'OP', 'parameter': 'G-8\x97\xbb&U\x03'} # ID; 472D3897BB265503
{'command': 'OI', 'parameter': '\x11u'}             # II; 11 75
{'command': 'CH', 'parameter': '\x12'}              # SC; 80
{'command': 'ZS', 'parameter': '\x00'}              # ZS; 00


Set the scan channels bitmask to enable the read operating channel (CH command). For example, if the operating channel is 0x0B, set SC to 0x0001. If the operating channel is 0x17, set SC to 0x1000.


Bit (Channel):	 0 (0x0B)   1 (0x0C)   2 (0x0D)   3 (0x0E)
                 4 (0x0F)   5 (0x10)   6 (0x11)   7 (0x12)
                 8 (0x13)   9 (0x14)  10 (0x15)  11 (0x16)
                12 (0x17) 13 (0x18) 14 (0x19) 15 (0x1A)

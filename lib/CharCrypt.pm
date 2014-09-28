#!/usr/bin/perl
#########################################################################################################
#Function:              �ַ����ܽ���ģ��
#Author:                WuGang
#Date Time:
#History:
#Copyright              2011 VanceInfo All Rights Reserved.
#########################################################################################################
package CharCrypt;
use strict;
use Crypt::RC4;

my $key = '12345678';

#rc4����
sub rc4Decrypt{
    my $encode = $_[0];                     #����
    if(!defined $encode or length $encode == 0){return '';}
    my $asciiStr;
    for(my $i = 0; $i< length $encode;$i=$i+2){
        $asciiStr .= chr(hex(substr($encode,$i,2)));
    }
    my $rc4    = Crypt::RC4->new($key);      #rc4����
    my $encrypted = $rc4->RC4($asciiStr);
    return $encrypted;
}

#rc4����
sub rc4Encrypt {
    my $plaintext = $_[0];                  #����
    my $rc4       = Crypt::RC4->new($key);
    my $encrypted = $rc4->RC4($plaintext);
    my $ascii = '';
    #ȡ����ASCIIֵ������ת��Ϊʮ��������ʽ
    for ( my $i = 0 ; $i < length $encrypted ; $i++ ) {
        my $hex = uc( sprintf( '%x', ord( substr( $encrypted, $i, 1 ) ) ) );
        if ( length $hex < 2 ) { $hex = '0'.$hex; }
        $ascii .= $hex;
    }
    return $ascii;
}

#!/usr/bin/perl
#########################################################################################################
#Function:              特殊日期模块
#Author:                WuGang
#Date Time:
#History:
#Copyright              2011 VanceInfo All Rights Reserved.
#########################################################################################################
package ETLDate;
use Time::Local;
use Logger;

$ETLDate::TODAY=undef;
$ETLDate::MONTH=undef;
$ETLDate::MAX_DAY=undef;                  #本月最大天数
$ETLDate::LAST_DAY=undef;                 #昨天
$ETLDate::MONTH_START=undef;              #月初
$ETLDate::MONTH_END=undef;                #月末
$ETLDate::LAST_MONTH_START=undef;         #上月月初
$ETLDate::LAST_MONTH_END=undef;           #上月月末
$ETLDate::LAST_2_MONTH_START=undef;       #上上月初
$ETLDate::LAST_2_MONTH_END=undef;         #上上月末
$ETLDate::LAST_3_MONTH_START=undef;       #上上上月初
$ETLDate::LAST_3_MONTH_END=undef;         #上上上月末
$ETLDate::YEAR_START=undef;               #今年年初
$ETLDate::YEAR_END=undef;                 #今年年末
$ETLDate::LAST_YEAR_END=undef;            #去年年末
$ETLDate::LAST_YEAR_SAME_DATE=undef;      #去年同期
$ETLDate::LAST_YEAR_NEXT_DATE=undef;      #去年同期+1天
$ETLDate::LAST_QUAR_START=undef;          #上季季初
$ETLDate::LAST_QUAR_END=undef;            #上季季末
$ETLDate::SEASON_START=undef;             #本季初
$ETLDate::SEASON_END=undef;               #本季末
$ETLDate::CUR_TENDAYS_START=undef;        #本旬初
$ETLDate::CUR_TENDAYS_END=undef;          #本旬末

$ETLDate::LAST_MONTH_NEXT_DAY=undef;      #当前日期减一个月 + 1
$ETLDate::LAST_3_MONTH_NEXT_DAY=undef;    #当前日期减三个月 + 1
$ETLDate::TEND_FIRST_DAY=undef;           #当前日期减十天 + 1

$ETLDate::NULL_DATE = "19000102";
$ETLDate::MIN_DATE  = "19000101";
$ETLDate::MAX_DATE  = "30001231";

#日期计算
#参数 $date   计算的参照日期
#参数 $offset 与参照日期的偏移量，正数表示以后的日期，负数表示以前的日期
sub dateCalc{
    my ( $date, $offset ) = @_;
    unless(isValidDate($date)){return undef;} #首先校验参考日期的有效性
    my $loc_year  = substr( $date, 0, 4 );
    my $loc_month = substr( $date, 4, 2 );
    my $loc_mday  = substr( $date, 6, 2 );
    $loc_month -= 1;
    my $timenum =
      Time::Local::timelocal( 0, 0, 0, $loc_mday, $loc_month, $loc_year );
    my @timestr = localtime( $timenum + 86400 * $offset );
    my $year    = $timestr[5] + 1900;
    my $month   = $timestr[4] + 1;
    my $mday    = $timestr[3];

    #格式化为YYYYMMDD的形式
    my $yyyymmdd = sprintf( "%04d%02d%02d", ${year}, ${month}, ${mday} );
    return $yyyymmdd;
}

#校验 yyyymmdd 日期格式的有效性 有效返回 1 无效返回 0
sub isValidDate{
    my $date = $_[0];
    #长度必须为8且八位日期必须为数字
    unless((length($date) == 8) and ($date =~ m/\d{8}/)){return 0}
    my $yyyy = substr($date,0,4);
    my $mm = substr($date,4,2);
    my $dd = substr($date,6,2);
    my %yyyymmdd=(
        "01"=>31,
        "03"=>31,
        "05"=>31,
        "07"=>31,
        "08"=>31,
        "10"=>31,
        "12"=>31,
        "04"=>30,
        "06"=>30,
        "09"=>30,
        "11"=>30
    );
    #判断闰年
    if(($yyyy % 400 == 0) or ($yyyy % 4 == 0 and $yyyy % 100 != 0)){
        $yyyymmdd{"02"} = 29;
    }else{
        $yyyymmdd{"02"} = 28;
    }

    if(!exists $yyyymmdd{$mm}){return 0;}   #月份必须在[01,12]区间
    my $maxDD = $yyyymmdd{$mm};             #该月份最大的日期

    if($dd >0 and $dd <= $maxDD){
        return 1;
    }else{
        return 0;
    }
}

#参数 1 $tx_date          参考日期
#参数 2 $cast_date_flag   是否转换日期格式标志，1则转换
sub init {
    my ($tx_date,$cast_date_flag) = @_;
    if ( !isValidDate($tx_date) ) {
        fatal("$tx_date is not a valid date");
    }
#   init 前 清除各变量
    $ETLDate::TODAY=undef;
    $ETLDate::MONTH=undef;
    $ETLDate::MAX_DAY=undef;                  #本月最大天数
    $ETLDate::LAST_DAY=undef;                 #昨天
    $ETLDate::MONTH_START=undef;              #月初
    $ETLDate::MONTH_END=undef;                #月末
    $ETLDate::LAST_MONTH_START=undef;         #上月月初
    $ETLDate::LAST_MONTH_END=undef;           #上月月末
    $ETLDate::LAST_2_MONTH_START=undef;       #上上月初
    $ETLDate::LAST_2_MONTH_END=undef;         #上上月末
    $ETLDate::LAST_3_MONTH_START=undef;       #上上上月初
    $ETLDate::LAST_3_MONTH_END=undef;         #上上上月末
    $ETLDate::YEAR_START=undef;               #今年年初
    $ETLDate::YEAR_END=undef;                 #今年年末
    $ETLDate::LAST_YEAR_END=undef;            #去年年末
    $ETLDate::LAST_YEAR_SAME_DATE=undef;      #去年同期
    $ETLDate::LAST_YEAR_NEXT_DATE=undef;      #去年同期+1天
    $ETLDate::LAST_QUAR_START=undef;          #上季季初
    $ETLDate::LAST_QUAR_END=undef;            #上季季末
    $ETLDate::SEASON_START=undef;             #本季初
    $ETLDate::SEASON_END=undef;               #本季末
    $ETLDate::CUR_TENDAYS_START=undef;        #本旬初
    $ETLDate::CUR_TENDAYS_END=undef;          #本旬末

    $ETLDate::LAST_MONTH_NEXT_DAY=undef;      #当前日期减一个月 + 1
    $ETLDate::LAST_3_MONTH_NEXT_DAY=undef;    #当前日期减三个月 + 1
    $ETLDate::TEND_FIRST_DAY=undef;           #当前日期减十天 + 1

    $ETLDate::NULL_DATE = "19000102";
    $ETLDate::MIN_DATE  = "19000101";
    $ETLDate::MAX_DATE  = "30001231";

    my $year  = substr( $tx_date, 0, 4 );
    my $month = substr( $tx_date, 4, 2 );
    my $day   = substr( $tx_date, 6, 2 );
    my $pre_year  = getPreYear($year);
    my $pre_month = getPreMonth($month);

    $ETLDate::TODAY = $tx_date;
    $ETLDate::MONTH = "$year$month";

    #本月最大天数
    $ETLDate::MAX_DAY = getMaxDay($year,$month);
    #   去年年末
    $ETLDate::LAST_YEAR_END = $pre_year . "1231";

    #上月月初、上月月末
    if ( $month eq '01' ) {
        $ETLDate::LAST_MONTH_START = $pre_year.$pre_month."01";
        $ETLDate::LAST_MONTH_END = $ETLDate::LAST_YEAR_END;
    }
    else {
        $ETLDate::LAST_MONTH_START = $year.$pre_month. "01";
        $ETLDate::LAST_MONTH_END   = $year.$pre_month.getMaxDay($year,$pre_month);
    }
    #当前日期减一个月 + 1
    my $last_month_max_day = getMaxDay(substr($ETLDate::LAST_MONTH_START,0,4),substr($ETLDate::LAST_MONTH_START,4,2));
    if(($day+1)>$last_month_max_day){
        $ETLDate::LAST_MONTH_NEXT_DAY = "$year$month".'01';
    }else{
        $ETLDate::LAST_MONTH_NEXT_DAY = dateCalc(substr($ETLDate::LAST_MONTH_START,0,6)."$day",1);
    }


    #   今年年初
    $ETLDate::YEAR_START = $year . '0101';

    #   去年同期
    if(isValidDate($pre_year.$month.$day)){
        $ETLDate::LAST_YEAR_SAME_DATE = $pre_year.$month.$day;
    }else{
        $ETLDate::LAST_YEAR_SAME_DATE = $pre_year.$month.getMaxDay($pre_year,$month);
    }
    $ETLDate::LAST_YEAR_NEXT_DATE = dateCalc($ETLDate::LAST_YEAR_SAME_DATE,1);

    #   月初
    $ETLDate::MONTH_START = $year . $month . '01';

    #   月末
    $ETLDate::MONTH_END = $year.$month.getMaxDay($year,$month);

    #今年年末
    $ETLDate::YEAR_END = $year."1231";
    #上季季初、上季季末、本季初、本季末
    if($month<=3){
        $ETLDate::LAST_QUAR_START = $pre_year."1001";
        $ETLDate::LAST_QUAR_END = $pre_year."1231";
        $ETLDate::SEASON_START = $year."0101";
        $ETLDate::SEASON_END = $year."0331";
    }elsif($month<=6){
        $ETLDate::LAST_QUAR_START = ($year)."0101";
        $ETLDate::LAST_QUAR_END = ($year)."0331";
        $ETLDate::SEASON_START = $year."0401";
        $ETLDate::SEASON_END = $year."0630";
    }elsif($month<=9){
        $ETLDate::LAST_QUAR_START = ($year)."0401";
        $ETLDate::LAST_QUAR_END = ($year)."0630";
        $ETLDate::SEASON_START = $year."0701";
        $ETLDate::SEASON_END = $year."0930";
    }elsif($month<=12){
        $ETLDate::LAST_QUAR_START = ($year)."0701";
        $ETLDate::LAST_QUAR_END = ($year)."0930";
        $ETLDate::SEASON_START = $year."1001";
        $ETLDate::SEASON_END = $year."1231";
    }
    #上上月初、上上月末
    if($month eq '01'){
        $ETLDate::LAST_2_MONTH_START = $pre_year."1101";
        $ETLDate::LAST_2_MONTH_END = $pre_year."1130";
    }elsif($month eq '02'){
        $ETLDate::LAST_2_MONTH_START = $pre_year."1201";
        $ETLDate::LAST_2_MONTH_END = $pre_year."1231";
    }else{
        my $tmp_month = getPreMonth($pre_month);
        $ETLDate::LAST_2_MONTH_START = $year.$tmp_month."01";
        $ETLDate::LAST_2_MONTH_END = $year.$tmp_month.getMaxDay($year,$tmp_month);
    }
    #上上上月初、上上上月末
    my $last_2_month = substr($ETLDate::LAST_2_MONTH_START,4,2);
    if($last_2_month eq '01'){
        $ETLDate::LAST_3_MONTH_START = $pre_year."1101";
        $ETLDate::LAST_3_MONTH_END = $pre_year."1130";
    }elsif($last_2_month eq '02'){
        $ETLDate::LAST_3_MONTH_START = $pre_year."1201";
        $ETLDate::LAST_3_MONTH_END = $pre_year."1231";
    }else{
        my $tmp_month = getPreMonth($last_2_month);
        $ETLDate::LAST_3_MONTH_START = $year.$tmp_month."01";
        $ETLDate::LAST_3_MONTH_END = $year.$tmp_month.getMaxDay($year,$tmp_month);
    }
    #当前日期减三个月 + 1
    my $last_3_max_day = getMaxDay(substr($ETLDate::LAST_3_MONTH_START,0,4),substr($ETLDate::LAST_3_MONTH_START,4,2));
    if(($day+1)>$last_3_max_day){
        $ETLDate::LAST_3_MONTH_NEXT_DAY = $ETLDate::LAST_2_MONTH_START;
    }else{
        $ETLDate::LAST_3_MONTH_NEXT_DAY = dateCalc(substr($ETLDate::LAST_3_MONTH_START,0,6)."$day",1);
    }


    #本旬初、本旬末
    if($day<=10){
        $ETLDate::CUR_TENDAYS_START = $year.$month."01";
        $ETLDate::CUR_TENDAYS_END = $year.$month."10";
    }elsif($day<=20){
        $ETLDate::CUR_TENDAYS_START = $year.$month."11";
        $ETLDate::CUR_TENDAYS_END = $year.$month."20";
    }else{
        $ETLDate::CUR_TENDAYS_START = $year.$month."21";
        $ETLDate::CUR_TENDAYS_END = $year.$month.getMaxDay($year,$month);
    }
    #昨天
    $ETLDate::LAST_DAY = dateCalc($tx_date,-1);
    #当前日期减十天 + 1
    $ETLDate::TEND_FIRST_DAY = dateCalc($tx_date,-9);
    if($cast_date_flag && $cast_date_flag eq '1'){
        initCastDate();
    }
}

sub getPreYear {
    my $year = shift;
    my $preYear = $year - 1;
    if($preYear>=1000){
        return $preYear;
    }else{
        return sprintf("%04d",$preYear);
    }
}

sub getPreMonth {
    my $month = shift;
    $month = $month + 0;
    if ( $month == 1 ) {
        return '12';
    }
    elsif ( $month <= 10 ) {
        return '0' . ( $month - 1 );
    }
    else {
        return $month - 1;
    }
}

sub getMaxDay{
    my $yyyy = shift;
    my $mm = shift;
    if(length($mm)==1){
        $mm = "0".$mm;
    }
    my %yyyymmdd=(
        "01"=>31,
        "03"=>31,
        "05"=>31,
        "07"=>31,
        "08"=>31,
        "10"=>31,
        "12"=>31,
        "04"=>30,
        "06"=>30,
        "09"=>30,
        "11"=>30
    );
    #判断闰年
    if(($yyyy % 400 == 0) or ($yyyy % 4 == 0 and $yyyy % 100 != 0)){
        $yyyymmdd{"02"} = 29;
    }else{
        $yyyymmdd{"02"} = 28;
    }
    return $yyyymmdd{$mm};
}
#CAST('$tx_date' AS DATE FORMAT 'YYYYMMDD')
sub initCastDate {
    $ETLDate::LAST_MONTH_NEXT_DAY   = castDate($ETLDate::LAST_MONTH_NEXT_DAY  );
    $ETLDate::LAST_3_MONTH_NEXT_DAY = castDate($ETLDate::LAST_3_MONTH_NEXT_DAY);
    $ETLDate::TEND_FIRST_DAY        = castDate($ETLDate::TEND_FIRST_DAY       );

    $ETLDate::MONTH_START           = castDate($ETLDate::MONTH_START          );
    $ETLDate::MONTH_END             = castDate($ETLDate::MONTH_END            );
    $ETLDate::LAST_MONTH_START      = castDate($ETLDate::LAST_MONTH_START     );
    $ETLDate::LAST_MONTH_END        = castDate($ETLDate::LAST_MONTH_END       );
    $ETLDate::LAST_2_MONTH_START    = castDate($ETLDate::LAST_2_MONTH_START );
    $ETLDate::LAST_2_MONTH_END      = castDate($ETLDate::LAST_2_MONTH_END   );
    $ETLDate::YEAR_START            = castDate($ETLDate::YEAR_START           );
    $ETLDate::YEAR_END              = castDate($ETLDate::YEAR_END             );
    $ETLDate::LAST_YEAR_END         = castDate($ETLDate::LAST_YEAR_END        );
    $ETLDate::LAST_YEAR_SAME_DATE   = castDate($ETLDate::LAST_YEAR_SAME_DATE  );
    $ETLDate::LAST_YEAR_NEXT_DATE   = castDate($ETLDate::LAST_YEAR_NEXT_DATE  );
    $ETLDate::LAST_QUAR_START       = castDate($ETLDate::LAST_QUAR_START      );
    $ETLDate::LAST_QUAR_END         = castDate($ETLDate::LAST_QUAR_END        );
    $ETLDate::SEASON_START          = castDate($ETLDate::SEASON_START         );
    $ETLDate::SEASON_END            = castDate($ETLDate::SEASON_END           );
    $ETLDate::CUR_TENDAYS_START     = castDate($ETLDate::CUR_TENDAYS_START    );
    $ETLDate::CUR_TENDAYS_END       = castDate($ETLDate::CUR_TENDAYS_END      );

    $ETLDate::NULL_DATE             = castDate($ETLDate::NULL_DATE            );
    $ETLDate::MIN_DATE              = castDate($ETLDate::MIN_DATE             );
    $ETLDate::MAX_DATE              = castDate($ETLDate::MAX_DATE             );

    $ETLDate::TODAY                 = castDate($ETLDate::TODAY                );
    $ETLDate::LAST_DAY              = castDate($ETLDate::LAST_DAY             );
}

sub castDate{
    my $date = shift;
    if($date){
        return "CAST('$date' AS DATE FORMAT 'YYYYMMDD')";
    }else{
        print "error\n";
    }
}

1;

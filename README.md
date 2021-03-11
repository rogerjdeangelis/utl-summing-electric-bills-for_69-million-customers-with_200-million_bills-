# utl-summing-electric-bills-for_69-million-customers-with_200-million_bills-
Summing electric bills for 69 million customers with 200 million bills

    Summing electric bills for 69 million customers with 200 million bills in 100 tables

    In less than 10 minutes

    GitHub
    https://tinyurl.com/m4wvy9k
    https://github.com/rogerjdeangelis/utl-summing-electric-bills-for_69-million-customers-with_200-million_bills-

    Inspired by
    https://tinyurl.com/35de289y
    https://stackoverflow.com/questions/66452293/sas-union-distinct-records-from-datasets-with-similar-names

    Below is a run with 100 tables each with about 2 million
    customer electric bills and about 1 million unique customers per table.

    The Total number of records in the 100 tables is over 200 million records.

    The final result is about 70 million distinct customers
    with their total electric bills.

    In some ways this is a worst case, because of the random nature of
    customer id. You have very poor locality of reference.

    Probably quite a bit faster with a hash and suminc.
    However, I can live with 10 minutes.
    Partial HASH solution on end.

    FYI
    It looks like you may be page thrashing in your attempts because you may
    not have enough ram.

    Since you ran out of memory at 23,068,656 items with two 8 byte variables.

    It looks like you ran out of ram at about

       23068656*16 bytes

       369mb of ram

    I have seen servers that restrict ram to less than 1 gb per user
    or are servers that is ram starved.

    Thrashing can make a 10-minute job take 10 hours.

    I cannot handle big data (single table over 1TB) on my very old
    Dell E6420 laptop. This is not one of the newer skinny laptops.

      Explanation
      ===========
          a. Generate 100 tables each with about 2 million electric bills
             for about 1 million distinct customers.
             (takes about 15 seconds to create all the tables)

          b. Run 10 parallel batch jobs
             Each batch job processes 10 tables
                What one batch job looks like
                       create a view of tables 1-10

                       data tblVue/view=tblVue;
                          set table1-table10
                       proc summary tblVue
                          class customer;
                          var bill
                Repeat 9 more times for each remaining sets on 10 tables

           c. Run this final job
                create view of ten output tables above
                proc summary tblVue
                class customer;
                var bill

    *_                   _
    (_)_ __  _ __  _   _| |_
    | | '_ \| '_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    ;

    * create 100 tables (15 seconds);

    You need an autocall folder. I use c:/oto;

    libname sd1 "d:/sd1";

    %array(dsns,values=1-100);

    %do_over(dsns,phrase=%str(
       data sd1.d?;
          do i=1 to 1200000;

             id=int(100000000*uniform(?))+1;

             if uniform(1234) <.80 then do;
                do j=1 to 2;
                   bil=int(1000*uniform(1357));
                   output;
                end;
              end;
              else output;
          end;
          drop i j;
       run;quit;
     ));

    *
     _ __  _ __ ___   ___ ___  ___ ___
    | '_ \| '__/ _ \ / __/ _ \/ __/ __|
    | |_) | | | (_) | (_|  __/\__ \__ \
    | .__/|_|  \___/ \___\___||___/___/
    |_|
    ;

    * macro to work on 10 tables at a time;
    * save view / summary code macro sysSum in your autocall folder;
    filename ft15f001 "c:/oto/syssum.sas";
    parmcards4;
    %macro syssum(grp);

    libname sd1 "d:/sd1";

       %array(grp,values=&grp);

       data d/view=d;
         set

          %do_over(grp,phrase=
             sd1.d?
           );

           run;quit;

       %let sfx=%scan(&grp,1,"-");

       proc summary data=d nway;
       class id;
       var bil;
       output out=sd1.want&sfx (drop=_type_ _freq_) sum=sumcnt;
       run;quit;

    %mend syssum;
    ;;;;
    run;quit;

    * refresh the macro in work catalog sasmacr * you need to run
    this include each time you change macro sysSum;;
    %inc "c:/oto/syssum.sas";

    * test interactively with just two of the 100 tables;;
    %syssum(1-2);

    * run 10 batch jobs each processing 10 tables;

    %let _s=%sysfunc(compbl(C:\Progra~1\SASHome\SASFoundation\9.4\sas.exe -sysin c:\oto\dummy.sas -sasautos c:\oto -rsasuser
     -config c:\cfg\cfgsas.cfg));

    %let tym=%sysfunc(time());
    %put &=tym;
    systask kill sys1 sys2 sys3 sys4 sys5 sys6 sys7 sys8 sys9 sys10;
    systask command "&_s -termstmt %nrstr(%syssum(1-10);) -log d:\log\syssuml.log" taskname=sys1;
    systask command "&_s -termstmt %nrstr(%syssum(11-20);) -log d:\log\syssum2.log" taskname=sys2;
    systask command "&_s -termstmt %nrstr(%syssum(21-30);) -log d:\log\syssum3.log" taskname=sys3;
    systask command "&_s -termstmt %nrstr(%syssum(31-40);) -log d:\log\syssum4.log" taskname=sys4;
    systask command "&_s -termstmt %nrstr(%syssum(41-50);) -log d:\log\syssum5.log" taskname=sys5;
    systask command "&_s -termstmt %nrstr(%syssum(51-60);) -log d:\log\syssum6.log" taskname=sys6;
    systask command "&_s -termstmt %nrstr(%syssum(61-70);) -log d:\log\syssum7.log" taskname=sys7;
    systask command "&_s -termstmt %nrstr(%syssum(71-80);) -log d:\log\syssum8.log" taskname=sys8;
    systask command "&_s -termstmt %nrstr(%syssum(81-90);) -log d:\log\syssum90.log" taskname=sys9;
    systask command "&_s -termstmt %nrstr(%syssum(91-100);) -log d:\log\syssum10.log" taskname=sys10;
    waitfor _all_ sys1 sys2 sys3;
    systask list;
    %put %sysevalf( %sysfunc(time()) - &tym);

    /* 197 seconds 3 minutes 17 seconds */

    * conatenate the 10 output tables from the batch runs above;

    data wantvue/view=wantvue;
      set sd1.want:;
      by id;
    run;quit;

    * final summary - note summary is multithreaded but has is not;
    proc summary data=wantvue nway;
    by id;
    var sumcnt;
    output out=want (drop=_type_ _freq_) sum=sumbil;
    run;quit;

    OTE: PROCEDURE SUMMARY used (Total process time):
          real time           5:08.15  /* IO bound? */
          user cpu time       3:35.26
          system cpu time     1:30.13

    *            _               _
      ___  _   _| |_ _ __  _   _| |_
     / _ \| | | | __| '_ \| | | | __|
    | (_) | |_| | |_| |_) | |_| | |_
     \___/ \__,_|\__| .__/ \__,_|\__|
                    |_|
    ;
    WANT total obs=69,555,069

        Obs    ID    SUMBIL

          1     1      1328
          2     2      2303
          3     3       454
          4     4      4159
          5     5      1890
          6     6      1319
          7     7      5779
          8     8      2331
          9     9      1583
         10    10      8563
         11    11      2879
         12    12      1039
    ....

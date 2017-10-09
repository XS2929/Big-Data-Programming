package com.refactorlabs.cs378.assign3;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import junit.framework.Assert;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Unit test for the WordCount map-reduce program.
 *
 * @author BULBASAUR
 */
public class InvertedIndexTest {
    
    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;
    
    @Before
    public void setup() {
        System.setProperty("hadoop.home.dir", "/");
        InvertedIndex.MapClass mapper = new InvertedIndex.MapClass();
        InvertedIndex.ReduceClass reducer = new InvertedIndex.ReduceClass();
        
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }
    //tested String
    private static final String TEST_WORD = "Message-ID: <7072085.1075857497884.JavaMail.evans@thyme>   Date: Thu, 22 Mar 2001 02:19:00 -0800 (PST) From: jane.tholt@enron.com  To: rhenk@barbnet.com   Subject: Re: FW: April - October 2001 Bid package   Cc: jane.tholt@enron.com,youyoucheckitout@whatever.com,yeaIm@utexas.edu    Mime-Version: 1.0   Content-Type: text/plain; charset=us-ascii  Content-Transfer-Encoding: 7bit Bcc: jane.tholt@enron.com,hiimyou.utexas.edu   X-From: Jane M Tholt    X-To: \"Henk, Robert\" <RHenk@barbnet.com> @ ENRON    X-cc: Jane M Tholt  X-bcc:  X-Folder: \\Jane_Tholt_Jun2001\\Notes Folders\\Sent    X-Origin: Tholt-J   X-FileName: jtholt.nsf      Hi Robert               Enron North America is interested in  purchasing 1200 mmbtu/day  El Paso    Poker Lake-Waha  pool gas for April-October 2001 on a firm basis at Inside  FERC El Paso Keystone  Index +.0025.  Please call me if you have any    questions.";
//"Message-ID: <23551738.1075857497351.JavaMail.evans@thyme>    Date: Mon, 14 May 2001 03:41:00 -0700 (PDT) From: jane.tholt@enron.com  To: outlook.team@enron.com  Subject: Re: 2- SURVEY  INFORMATION EMAIL 5-14- 01  Mime-Version: 1.0   Content-Type: text  plain; charset=us-ascii Content-Transfer-Encoding: 7bit X-From: Jane M Tholt    X-To: Outlook Migration Team    X-cc:   X-bcc:  X-Folder:  Jane_Tholt_Jun2001 Notes Folders Sent    X-Origin: Tholt-J   X-FileName: jtholt.nsf  Full Name: Jane Marie Tholt     Login ID:  jtholt       Extension:  35539       Office Location:  eb3209c       What type of computer do you have?  (Desktop,  Laptop,  Both)  desktop      Do you have a PDA?  If yes, what type do you have:   (None, IPAQ, Palm Pilot,   Jornada)  no        Do you have permission to access anyone's Email  Calendar? no       If yes, who?        Does anyone have permission to access your Email  Calendar?  no     If yes, who?        Are you responsible for updating anyone else's address book?  no        If yes, who?        Is anyone else responsible for updating your address book?  no      If yes, who?        Do you have access to a shared calendar?        If yes, which shared calendar?  west desk calendar      Do you have any Distribution Groups that Messaging maintains for you (for   mass mailings)?         If yes, please list here:  no       Please list all Notes databases applications that you currently use:        In our efforts to plan the exact date  time of your migration, we also will     need to know:       What are your normal work hours?  From:7:00 a.m.     To:   5:00   p.m.      Will you be out of the office in the near future for vacation, leave, etc?         If so, when?        From (MM  DD  YY):  05  13  01    To (MM  DD  YY):       05  16  01";

    @Test
    public void testMapClass() {
        //there will be more than one output, check everyone
        mapDriver.withInput(new LongWritable(0L), new Text(TEST_WORD));
        mapDriver.withOutput(new Text("From:jane.tholt@enron.com"), new Text("<7072085.1075857497884.JavaMail.evans@thyme>"));
        mapDriver.withOutput(new Text("To:rhenk@barbnet.com"), new Text("<7072085.1075857497884.JavaMail.evans@thyme>"));
        mapDriver.withOutput(new Text("Cc:jane.tholt@enron.com"), new Text("<7072085.1075857497884.JavaMail.evans@thyme>"));
        mapDriver.withOutput(new Text("Cc:youyoucheckitout@whatever.com"), new Text("<7072085.1075857497884.JavaMail.evans@thyme>"));
        mapDriver.withOutput(new Text("Cc:yeaIm@utexas.edu"), new Text("<7072085.1075857497884.JavaMail.evans@thyme>"));
        mapDriver.withOutput(new Text("Bcc:jane.tholt@enron.com"), new Text("<7072085.1075857497884.JavaMail.evans@thyme>"));
        mapDriver.withOutput(new Text("Bcc:hiimyou.utexas.edu"), new Text("<7072085.1075857497884.JavaMail.evans@thyme>"));     
        try {
            mapDriver.runTest();
        } catch (IOException ioe) {
            Assert.fail("IOException from mapper: " + ioe.getMessage());
        }
    }
    
    @Test
    public void testReduceClass() {
        //test output
        List<Text> valueList = Lists.newArrayList(new Text("<10450485.1075857499511.JavaMail.evans@thyme>"),new Text("<23551738.1075857497351.JavaMail.evans@thyme>"),new Text("<98087.1075857499511.JavaMail.evans@thyme>"));
        reduceDriver.withInput(new Text("From:jane.tholt@enron.com"), valueList);
        reduceDriver.withOutput(new Text("From:jane.tholt@enron.com"), new Text("<10450485.1075857499511.JavaMail.evans@thyme>,<23551738.1075857497351.JavaMail.evans@thyme>,<98087.1075857499511.JavaMail.evans@thyme>"));
        try {
            reduceDriver.runTest();
        } catch (IOException ioe) {
            Assert.fail("IOException from mapper: " + ioe.getMessage());
        }
    }
}

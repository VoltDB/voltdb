#!/usr/bin/env groovy

import groovy.transform.Canonical
import java.text.SimpleDateFormat
import java.util.regex.*
import java.util.zip.GZIPInputStream

def logPFX = "^(\\d{4}-\\d{2}-\\d{2}\\s\\d{1,2}:\\d{2}:\\d{2},\\d{3})\\s+(TRACE|DEBUG|INFO|WARN|ERROR)\\s+(\\[[^]]+?\\])\\s+"

class LogFeeder {
	final ArrayList<String> records = new ArrayList<>()
	final Pattern logRE = ~/^\d{4}-\d{2}-\d{2}\s\d{1,2}:\d{2}:\d{2},\d{3}\s+/

	List<String> feed(String ln) {
		Matcher mtc = ln =~ logRE
		List<String> logEntry = Collections.emptyList();
		if (mtc.find(0) && records.size() > 0) {
        	logEntry = new ArrayList<String>(records)
        	records.clear()
        }
        records << ln
        logEntry
	} 
}

@Canonical
class Trigger {
	Pattern re
	Closure<?> onMatch
	final SimpleDateFormat dtfmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS Z")

	void probe(List<String> lines) {
		if (!lines) return
		Matcher mtc = lines[0] =~ re
		if (mtc.find(0)) {
			long tm = dtfmt.parse("${mtc.group(1)} UTC").time
			onMatch(lines, mtc, tm)
		}
	}
}

@Canonical
class Labeler {
	int count = 0
	String getLabel() {
		Integer.toString(++count, Character.MAX_RADIX)
	}
}

@Canonical
class Row {
	long date
    double three9s
    double four9s
    double five9s
	int errors
	String mark

	TreeSet<String> titles = new TreeSet<String>()
	TreeSet<String> texts = new TreeSet<String>()
	Row annotate( String title, String text) {
		if(title) titles << title
		if(text) texts << text
		this
	}
	Row annotate( String label, String title, String text) {
		if(label && !mark) mark = label
		if(title) titles << title
		if(text) texts << text
		this
	}
	String getText() {
		titles ? "'${titles.join(',')} ${texts.join(',')}'" : 'undefined'
	}
	String getAnnotation() {
		mark ? "'${mark}'" : 'undefined'
	}
    String getAnnotatedRow() {
        "[new Date(${date}), ${five9s}, ${annotation}, ${text}]"
    }
    String getNinesRow() {
        "[new Date(${date}), ${three9s}, ${four9s}, ${five9s}]"
    }
	void label(Labeler lblr) {
		if (titles && !mark) mark = lblr.label
	}
}

def cli = new CliBuilder(usage: 'chartbuilder.groovy [options] [log-files]')

cli.c(longOpt: 'csv-file', required:true, argName:'file', args:1, 'periodic measurements csv file' )
cli.h(longOpt: 'help', required:false, 'usage information')
cli.f(longOpt: 'five9s', required:false, argName:'file', args:1, 'fine nines chart output file [default: annotatedFive9s.html]')
cli.a(longOpt: 'all9s', required:false, argName:'file', args:1, 'all nines chart output file [default: all9s.html')

def opts = cli.parse(args)
if (!opts) return
else if (opts.h) {
	cli.usage()
	return
}

def five9sFN = "annotatedFive9s.html"
def all9sFN = "all9s.html"

if (opts.o) five9sFN = opts.o
if (opts.a) all9sFN = opts.a

def fpw = new PrintWriter(new FileWriter(five9sFN),true)
def apw = new PrintWriter(new FileWriter(all9sFN),true)

def periodicFH = new File(opts.c)

def data = [:] as TreeMap

new GZIPInputStream(new FileInputStream(periodicFH)).readLines()[1..-1].collectEntries(data) {
	s=it.split(',')
    [s[1] as long,
    new Row(
        s[1]  as long,   // timestamp
        s[9]  as double, // three nines
        s[10] as double, // four nines
        s[11] as double, // five nine
        s[3..5].collect { it as int }.inject(0) {a,i -> a+i} // aborts + errors + timeouts
    )]
}

def triggers = []
def node = ""

def noteWorthy = { long tm, String label, String event ->
	if (data.subMap(tm-2500,true,tm+2500,true).findAll {k,v -> v.five9s > 5}.size() > 0) {
		data.floorEntry(tm)?.value?.annotate(label,event,node)
	}
}

def snapinzRE = ~/${logPFX}SNAPSHOT:\sSnapshot\sinitiation\stook\s(\d+)\smilliseconds/

triggers << new Trigger(re: snapinzRE, onMatch: { lines, mtc, tm ->
	int dur = mtc.group(4) as int
	noteWorthy(tm,'P','snapinit')
})

def snapendRE = ~/${logPFX}SNAPSHOT:\sSnapshot\s(\d+)\sfinished\sat\s\d+\sand\stook\s(\d+(?:\.\d+)?)\sseconds/

triggers << new Trigger(re: snapendRE, onMatch: { lines, mtc, tm ->
	noteWorthy(tm,'P','snapend')
})

def segunavailRE = ~/${logPFX}Attempted\sto\sloan\sa\spreallocated\slog\ssegment/

triggers << new Trigger(re: segunavailRE, onMatch: { lines, mtc, tm ->
	noteWorthy(tm,'G','segunavail')
})

def segaddRE = ~/${logPFX}LOGGING:\sFinished\sadding\ssegment/

triggers << new Trigger(re: segaddRE, onMatch: { lines, mtc, tm ->
	noteWorthy(tm,'G','segadd')
})

def nodefailRE = ~/${logPFX}REJOIN:\sAgreement,\sAdding\s(\d+:-\d+)\sto\sfailed\ssites\shistory/

triggers << new Trigger(re: nodefailRE, onMatch: { lines, mtc, tm ->
	data.floorEntry(tm)?.value?.annotate('N','nodefail', node)
})

def feeder = new LogFeeder()
def voltRE = ~/(?i)(?:volt|-log)/

opts.arguments().each { logFN ->
	def logFH = new File(logFN)

	def fmtc = logFH.name[0..<logFH.name.indexOf('.')] =~ voltRE
	node = fmtc.replaceAll('')

	logFH.eachLine {ln ->
		
		def logEntry = feeder.feed(ln)
		if (logEntry) {
			triggers*.probe(logEntry)
		}
	}
}

def rows = data.values()

def txt = """<html>
  <head>
    <script type='text/javascript' src='http://www.google.com/jsapi'></script>
    <script type='text/javascript'>
      google.load("visualization", "1", {packages:["corechart"]});
      google.setOnLoadCallback(drawChart);
      function drawChart() {
        var data = new google.visualization.DataTable();
        data.addColumn('datetime', 'Date');
        data.addColumn('number', 'Five9s');
        data.addColumn({type: 'string', role: 'annotation'});
        data.addColumn({type: 'string', role: 'annotationText'});
        data.addRows([
          ${rows.collect {it.annotatedRow}.join(",\n          ")}
        ]);

        var chart = new google.visualization.LineChart(document.getElementById('chart_div'));

        var options = {
          title: 'KVBenchmark Five9s',
          explorer: { 
          	axis: 'horizontal',
          	maxZoomIn: 0.001, 
          	actions: ['dragToZoom', 'rightClickToReset']
          },
          hAxis : {title: 'Date' }
        };

        chart.draw(data, options);
      }
    </script>
  </head>

  <body>
    <div id='chart_div' style='width: 1350px; height: 750px;'></div>
  </body>
</html>
"""

fpw.println txt
fpw.close()

txt = """<html>
  <head>
    <script type='text/javascript' src='http://www.google.com/jsapi'></script>
    <script type='text/javascript'>
      google.load("visualization", "1", {packages:["corechart"]});
      google.setOnLoadCallback(drawChart);
      function drawChart() {
        var data = new google.visualization.DataTable();
        data.addColumn('datetime', 'Date');
        data.addColumn('number', 'Three9s');
        data.addColumn('number', 'Four9s');
        data.addColumn('number', 'Five9s');
        data.addRows([
          ${rows.collect {it.ninesRow}.join(",\n          ")}
        ]);

        var chart = new google.visualization.LineChart(document.getElementById('chart_div'));

        var options = {
          title: 'KVBenchmark All9s',
          explorer: {
            axis: 'horizontal',
            maxZoomIn: 0.001,
            actions: ['dragToZoom', 'rightClickToReset']
          },
          hAxis : {title: 'Date' }
        };

        chart.draw(data, options);
      }
    </script>
  </head>

  <body>
    <div id='chart_div' style='width: 1350px; height: 750px;'></div>
  </body>
</html>
"""

apw.print txt
apw.close()

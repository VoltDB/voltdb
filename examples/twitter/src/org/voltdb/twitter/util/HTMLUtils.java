package org.voltdb.twitter.util;

import java.util.LinkedList;
import java.util.List;

public class HTMLUtils {
    
    public List<String> header() {
        List<String> html = new LinkedList<String>();
        html.add("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Strict//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd\">");
        html.add("<html xmlns=\"http://www.w3.org/1999/xhtml\" lang=\"en\" xml:lang=\"en\">");
        html.add("<head>");
        html.add("\t<meta http-equiv=\"Content-Type\" content=\"text/html; charset=ISO-8859-1\" />");
        html.add("\t<meta http-equiv=\"refresh\" content=\"5\" />");
        html.add("\t<title>Twitter Trends</title>");
        html.add("\t<link rel=\"stylesheet\" type=\"text/css\" href=\"/style.css\" />");
        html.add("\t<link rel=\"shortcut icon\" href=\"http://voltdb.com/sites/all/themes/volt/favicon.ico\" type=\"image/x-icon\" />");
        html.add("</head>");
        html.add("<body>");
        return html;
    }
    
    // prints an html table from a hashtag/count list
    public List<String> table(List<HashTag> hashTags) {
        int max = hashTags.size() != 0 ? hashTags.get(0).getCount() : 0;
        
        List<String> html = new LinkedList<String>();
        html.add("<table class=\"hashtags\">");
        
        html.add("\t<thead>");
        html.add("\t\t<tr>");
        html.add("\t\t\t<th>hashtag</th>");
        html.add("\t\t\t<th>count</th>");
        html.add("\t\t</tr>");
        html.add("\t</thead>");
        
        html.add("\t<tbody>");
        for (HashTag hashTag : hashTags) {            
            int width = (int) ((double) hashTag.getCount() / max * 110);
            
            html.add("\t\t<tr>");
            html.add("\t\t\t<td>");
            html.add("\t\t\t\t<a href=\"http://search.twitter.com/search?q=%23" + hashTag.getHashTag() + "\">" + hashTag.getHashTag() + "</a>");
            html.add("\t\t\t</td>");
            html.add("\t\t\t<td>");
            html.add("\t\t\t\t<div style=\"position: absolute;\"><div class=\"hashtag-bar\" style=\"width: " + width + "px;\"></div></div>");
            html.add("\t\t\t\t<span class=\"hashtag-count\">" + hashTag.getCount() + "</span>");
            html.add("\t\t\t</td>");
            html.add("\t\t</tr>");
        }
        html.add("\t</tbody>");
        
        html.add("</table>");
        return html;
    }
    
    public List<String> image(String path, int width, int height, String alt) {
        List<String> html = new LinkedList<String>();
        html.add("<img src=\"" + path + "\" height=\"" + height + "\" width=\"" + width + "\" alt=\"" + alt + "\" />");
        return html;
    }
    
    public List<String> footer() {
        List<String> html = new LinkedList<String>();
        html.add("</body>");
        html.add("</html>");
        return html;
    }
    
}
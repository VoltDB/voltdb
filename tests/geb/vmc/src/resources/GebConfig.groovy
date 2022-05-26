/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package vmcTest.config

import org.openqa.selenium.chrome.ChromeDriver
import org.openqa.selenium.firefox.FirefoxDriver
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.openqa.selenium.ie.InternetExplorerDriver
import org.openqa.selenium.phantomjs.PhantomJSDriver
import org.openqa.selenium.safari.SafariDriver

import org.openqa.selenium.chrome.ChromeOptions
import org.openqa.selenium.firefox.FirefoxOptions
import org.openqa.selenium.remote.DesiredCapabilities

// Returns the specified project property value, if it is defined; otherwise,
// returns the specified default value
def getProjectPropertyOrDefaultValue(String projectPropertyName, Object defaultValue) {
    if (project.hasProperty(projectPropertyName)) {
        return project.getProperties()[projectPropertyName]
    } else {
        return defaultValue
    }
}

// Possible future work: get 'includes' like this working
//apply from: GetPropertyValues.gradle

waiting {
    timeout = getProjectPropertyOrDefaultValue("timeoutSeconds", 20)
}

environments {

/*
* As of 2018, phantomjs support is deprecated in favor of "headless" chrome or firefox.
* You start headless Chrome and Firefox by setting options.
* The run.sh converts "--headless" to an environment variable so we can
* run chrome in either mode.
*/

    firefox {
        def isHeadless = System.getenv("HEADLESS");
        if(isHeadless == "TRUE") {
            FirefoxOptions options = new FirefoxOptions()
            DesiredCapabilities capabilities = DesiredCapabilities.firefox()
            options.addArguments("-headless")
            //capabilities.setCapability(FirefoxOptions.CAPABILITY, options)
            driver = { new FirefoxDriver(options) }
         } else {
           driver = { new FirefoxDriver() }
        }
    }

    chrome {
        def isHeadless = System.getenv("HEADLESS");
        if(isHeadless == "TRUE") {
            ChromeOptions options = new ChromeOptions()
            DesiredCapabilities capabilities = DesiredCapabilities.chrome()
            options.addArguments("headless")
            capabilities.setCapability(ChromeOptions.CAPABILITY, options)
            driver = { new ChromeDriver(capabilities) }
         } else {
            driver = { new ChromeDriver() }
        };
    }


    ie {
        driver = { new InternetExplorerDriver() }
    }

    htmlunit {
        driver = { new HtmlUnitDriver(true) }
    }

    phantomjs {
        driver = { new PhantomJSDriver() }
    }

    safari {
/*
        baseURL = "http://localhost:8080/"
        def sel = new DefaultSelenium("localhost", 4444, "*safari", baseURL)
        CommandExecutor executor = new SeleneseCommandExecutor(sel)
        DesiredCapabilities dc = new DesiredCapabilities()
        dc.setBrowsername("safari")
        dc.setCapability("platform", org.openqa.selenium.Platform.MAC)
        dc.setJavascriptEnabled(true)
        driver = new RemoteWebDriver(executor, dc)
*/
        driver = { new SafariDriver() }
    }

}

// To run the tests with all browsers run “./gradlew test”

/**
 * Created by anrai on 2/12/15.
 */
package vmcTest.tests

import vmcTest.pages.*
import geb.Page;

class LoginLogoutTest extends TestBase {
    
    def "Login Test Valid Username and Password"() {
        given:
        to LoginLogoutPage

        def $line
        new File("src/pages/users.txt").withReader { $line = it.readLine() }

        when: "submitted"
        loginBoxuser1.value($line)
        loginBoxuser2.value($line)
        loginbtn.click()

        then:
        at DbMonitorPage
    }

    def "Login Test Valid Username and Invalid Password"() {
        given:
        to LoginLogoutPage

        def $line
        new File("src/pages/users.txt").withReader { $line = it.readLine() }

        when: "submitted"
        loginBoxuser1.value($line)
        loginBoxuser2.value("anything")
        loginbtn.click()

        then:
        at LoginLogoutPage
    }

    def "Login Test Valid Username and empty Password"() {
        given:
        to LoginLogoutPage

        def $line
        new File("src/pages/users.txt").withReader { $line = it.readLine() }

        when: "submitted"
        loginBoxuser1.value($line)
        loginBoxuser2.value("")
        loginbtn.click()

        then:
        at LoginLogoutPage
    }

    def "Login Test Invalid Username and Password"() {
        given:
        to LoginLogoutPage

        def $line
        new File("src/pages/users.txt").withReader { $line = it.readLine() }

        when: "submitted"
        loginBoxuser1.value("anything")
        loginBoxuser2.value("anything")
        loginbtn.click()

        then:
        at LoginLogoutPage
    }
    
    def "Login Test Invalid Username and Empty Password"() {
        given:
        to LoginLogoutPage

        def $line
        new File("src/pages/users.txt").withReader { $line = it.readLine() }

        when: "submitted"
        loginBoxuser1.value("anything")
        loginBoxuser2.value("")
        loginbtn.click()

        then:
        at LoginLogoutPage
    }
    

    def "Login Test Invalid Username and Valid Password"() {
        given:
        to LoginLogoutPage

        def $line
        new File("src/pages/users.txt").withReader { $line = it.readLine() }

        when: "submitted"
        loginBoxuser1.value("anything")
        loginBoxuser2.value($line)
        loginbtn.click()

        then:
        at LoginLogoutPage
    }

    def "Login Test Empty Username and Password"() {
        given:
        to LoginLogoutPage

        def $line
        new File("src/pages/users.txt").withReader { $line = it.readLine() }

        when: "submitted"
        loginBoxuser1.value("")
        loginBoxuser2.value("")
        loginbtn.click()

        then:
        at LoginLogoutPage
    }

    
    
    def "Login Test Empty Username and Valid Password"() {
        given:
        to LoginLogoutPage

        def $line
        new File("src/pages/users.txt").withReader { $line = it.readLine() }

        when: "submitted"
        loginBoxuser1.value("")
        loginBoxuser2.value($line)
        loginbtn.click()

        then:
        at LoginLogoutPage
    }

    def "Login Test Empty Username and Invalid Password"() {
        given:
        to LoginLogoutPage

        def $line
        new File("src/pages/users.txt").withReader { $line = it.readLine() }

        when: "submitted"
        loginBoxuser1.value("")
        loginBoxuser2.value("anything")
        loginbtn.click()

        then:
        at LoginLogoutPage
    }


}

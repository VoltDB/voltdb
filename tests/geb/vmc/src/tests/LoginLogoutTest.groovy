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
        def $line1
        new File("src/resources/users.txt").withReader { $line = it.readLine()
            $line1=it.readLine()}

        when: "submitted"
        loginBoxuser1.value($line)
        loginBoxuser2.value($line1)


        then:
        loginbtn.click()
        println("username and password valid")

    }

    def "Login Test Valid Username and Invalid Password"() {
        given:
        to LoginLogoutPage

        def $line
        new File("src/resources/users.txt").withReader { $line = it.readLine() }

        when: "submitted"
        loginBoxuser1.value($line)
        loginBoxuser2.value("wrong password")


        then:
        at LoginLogoutPage
        loginbtn.click()
        println("valid username and invalid password")
    }

    def "Login Test Valid Username and empty Password"() {
        given:
        to LoginLogoutPage

        def $line
        new File("src/resources/users.txt").withReader { $line = it.readLine() }

        when: "submitted"
        loginBoxuser1.value($line)
        loginBoxuser2.value("")


        then:
        at LoginLogoutPage
        loginbtn.click()
        println("valid username and empty password")
    }

    def "Login Test Invalid Username and Password"() {
        given:
        to LoginLogoutPage

        def $line
        new File("src/resources/users.txt").withReader { $line = it.readLine() }

        when: "submitted"
        loginBoxuser1.value("anything")
        loginBoxuser2.value("anything")


        then:
        at LoginLogoutPage
        loginbtn.click()
        println("invalid username and invalid password")
    }

    def "Login Test Invalid Username and Empty Password"() {
        given:
        to LoginLogoutPage

        def $line
        new File("src/resources/users.txt").withReader { $line = it.readLine() }

        when: "submitted"
        loginBoxuser1.value("anything")
        loginBoxuser2.value("")


        then:
        at LoginLogoutPage
        loginbtn.click()
        println("invalid username and empty password")
    }


    def "Login Test Invalid Username and Valid Password"() {
        given:
        to LoginLogoutPage

        def $line
        def $line1
        new File("src/resources/users.txt").withReader { $line = it.readLine()
            $line1=it.readLine() }

        when: "submitted"
        loginBoxuser1.value("anything")
        loginBoxuser2.value($line1)


        then:
        at LoginLogoutPage
        loginbtn.click()
        println("invalid username and valid password")
    }

    def "Login Test Empty Username and Password"() {
        given:
        to LoginLogoutPage

        def $line
        new File("src/resources/users.txt").withReader { $line = it.readLine() }

        when: "submitted"
        loginBoxuser1.value("")
        loginBoxuser2.value("")


        then:
        at LoginLogoutPage
        loginbtn.click()
        println("empty username and empty password")
    }



    def "Login Test Empty Username and Valid Password"() {
        given:
        to LoginLogoutPage

        def $line
        def $line1
        new File("src/resources/users.txt").withReader { $line = it.readLine()
            $line1= it.readLine() }

        when: "submitted"
        loginBoxuser1.value("")
        loginBoxuser2.value($line)


        then:
        at LoginLogoutPage
        loginbtn.click()
        println("empty username and valid password")
    }

    def "Login Test Empty Username and Invalid Password"() {
        given:
        to LoginLogoutPage

        def $line
        new File("src/resources/users.txt").withReader { $line = it.readLine() }

        when: "submitted"
        loginBoxuser1.value("")
        loginBoxuser2.value("anything")


        then:
        at LoginLogoutPage
        loginbtn.click()
        println("empty username and invalid password")
    }


}

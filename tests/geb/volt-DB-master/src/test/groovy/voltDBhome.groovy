
import geb.Page

class voltDBhome extends Page {

    // enter URL from setbaseURL.txt file that resides in "src/test/groovy" folder
    // for dynamic approach

//static url =new File("src/test/groovy/setbaseURL.txt").withReader { url = it.readLine() }

   /* This is the baseURL of voltDB*/

    static url="http://192.168.0.213:8080/"

    static at = {



        waitFor { loginBoxuser1.isDisplayed() }           // variable declaration for username
        waitFor { loginBoxuser2.isDisplayed() }         //variable declaration for password
        waitFor { loginbtn.isDisplayed()      }       // variable declaration for login button




    }


    static content = {


        loginBoxuser1 {

            $("input", type: "text", name: "username")
        }

            loginBoxuser2 {

            $("input", type: "password", name: "password")
        }



        loginbtn {
            $("input", type: "submit", id: "LoginBtn", value: "Login")


        }



    }

}

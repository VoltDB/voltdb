import React, { Component } from 'react';
import './App.css';

import Pages from './components/pages.jsx';
// import Fullpage from './components/fullpage.jsx';
import JSONP from 'browser-jsonp';

// Root element
class App extends Component {

  constructor(props) {
    super(props);
    this.state = {
      num: 1001
    };

    // Update the total number of swipes
    setInterval(() => {
      // let url = "http://localhost:8080/api/1.0/?Procedure=@AdHoc&Parameters=['select count(*) from activity']";
      JSONP({
        url: 'http://localhost:8080/api/1.0/',
        data: { Procedure: '@AdHoc', Parameters: "['select count(*) from activity']" },
        success: (data) => {
          this.setState({ num: data.results[0].data[0][0] });
        },
        error:(err) => { console.log(err); },
        callbackName: 'jsonp' // Important !
      });
    }, 2000);
  }

  render() {
    return (
      <div>
        <Pages num={this.state.num} theme={'car'} />
      </div>
    );
  }
}

export default App;
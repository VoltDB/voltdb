import React, { Component } from 'react';
import './App.css';

import Pages from './components/pages.jsx';
// import Fullpage from './components/fullpage.jsx';

// Root element
class App extends Component {

  constructor(props) {
    super(props);
    this.state = {
      num: 1001
    };

    setInterval(() => {
      this.setState({num: this.state.num + 1000});
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
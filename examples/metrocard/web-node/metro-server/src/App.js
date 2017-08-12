import React, { Component } from 'react';
// import logo from './logo.svg';
import './App.css';

import NavbarInstance from './components/navbar.jsx';
import OdometerInstance from './components/odometer.jsx';

// Root element
class App extends Component {

  constructor(props) {
    super(props);
    this.state = {
      num: 1234
    };

    setInterval(() => {
      var prev = this.state.num;
      this.setState({ num: prev + 1000 });
    }, 2000);
  }

  render() {
    return (
      <div>
        <NavbarInstance />
        <div>
          <OdometerInstance value={this.state.num} />
        </div>
      </div>
    );
  }
}

export default App;
import React, { Component } from 'react';

import '../components-css/fullpage.css';
var $ = require('jquery')(require('jsdom').jsdom().defaultView);

class Fullpage extends Component {
  constructor(props) {
    super(props);
    $('#fullpage').fullpage();
  }

  render() {
    return (
      <div id="fullpage">
        <div className="section">Some section</div>
        <div className="section">Some section</div>
        <div className="section">Some section</div>
        <div className="section">Some section</div>
      </div>
    );
  }
}

export default Fullpage;
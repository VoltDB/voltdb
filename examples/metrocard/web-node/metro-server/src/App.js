import React, { Component } from 'react';
import './App.css';

import Pages from './components/pages.jsx';
// import Fullpage from './components/fullpage.jsx';
import JSONP from 'browser-jsonp';

const data = new Array(10).fill(0).reduce((prev, curr) => [...prev, {
  x: prev.slice(-1)[0].x + 1,
  y: prev.slice(-1)[0].y * (1 + Math.random())
}], [{ x: 0, y: 10 }])

// Root element
class App extends Component {

  constructor(props) {
    super(props);
    this.state = {
      num: 1001, // total # of swipes
      frauds: 0,  // detected frauds
      busiestStations: data, // busiest stations
      avgWaits: []
    };

    // Update the data
    setInterval(() => {
      // Get the total number of swipes
      JSONP({
        url: 'http://localhost:8080/api/1.0/',
        data: { Procedure: '@AdHoc', Parameters: "['select count(*) from activity']" },
        success: (data) => {
          this.setState({ num: data.results[0].data[0][0] });
        },
        error: (err) => { console.log(err); },
        callbackName: 'jsonp' // Important !
      });

      // Get the swipes for the busiest stations
      JSONP({
        url: 'http://localhost:8080/api/1.0/',
        data: { Procedure: 'GetBusiestStationInLastMinute' },
        success: (data) => {
          let newData = data.results[0].data;
          newData = newData.map((v, i, arr) => {
            let r = {};
            r.x = v[0];
            r.y = v[1];
            return r;
          });

          this.setState({
            busiestStations: newData
          });
        },
        error: (err) => { console.log(err); },
        callbackName: 'jsonp' // Important !
      });

      // Get the info for avg waiting time
      JSONP({
        url: 'http://localhost:8080/api/1.0/',
        data: { Procedure: 'GetStationWaitTime' },
        success: (data) => {
          let newData = data.results[0].data;
          newData = newData.map((v, i, arr) => {
            let r = {};
            r.x = v[0];
            r.y = v[1];
            return r;
          });

          this.setState({
            avgWaits: newData
          });
        },
        error: (err) => { console.log(err); },
        callbackName: 'jsonp' // Important !
      });
    }, 2000);
  }

  render() {
    return (
      <div>
        <Pages num={this.state.num} busiestStations={this.state.busiestStations} avgWaits={this.state.avgWaits} theme={'car'} />
      </div>
    );
  }
}

export default App;
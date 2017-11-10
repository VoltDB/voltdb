import React, { Component } from 'react';
import './App.css';

import Pages1 from './components/pages1.jsx';
import Pages2 from './components/pages2.jsx';
import Pages3 from './components/pages3.jsx';
import Pages4 from './components/pages4.jsx';
// import Fullpage from './components/fullpage.jsx';
import JSONP from 'browser-jsonp';

const data = new Array(10).fill(0).reduce((prev, curr) => [...prev, {
  x: prev.slice(-1)[0].x + 1,
  y: prev.slice(-1)[0].y * (1 + Math.random())
}], [{ x: 0, y: 10 }])

// Root element
// TODO: setup routing ?
class App extends Component {

  constructor(props) {
    super(props);
    this.curSection = 1;
    this.state = {
      num: 0, // total # of swipes
      frauds: 0,  // detected frauds
      busiestStations: data, // busiest stations
      avgWaits: [], // the average wait times
      rate: 50.00
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

      // Get the card swipe acceptance rate
      JSONP({
        url: 'http://localhost:8080/api/1.0/',
        data: { Procedure: 'GetCardAcceptanceRate' },
        success: (data) => {
          this.setState({ frauds: data.results[0].data[0][0] });
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


      JSONP({
        url: 'http://localhost:8080/api/1.0/',
        data: { Procedure: 'UpdateWaitTime' , Parameters: "[ '9000' ]" },
        error: (err) => { console.log(err); },
        callbackName: 'jsonp' // Important !
      });

      JSONP({
        url: 'http://localhost:8080/api/1.0/',
        data: { Procedure: 'GetStationWaitTime' },
        success: (data) => {
          let newData = data.results[0].data;
          newData = newData.map((v, i, arr) => {
            let r = {};
            r.x = v[0];
            r.y = v[1] === 0 ? 0 : v[1] / v[2];
            return r;
          });

          this.setState({
            avgWaits: newData
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
            r.y = v[1] === 0 ? 0 : v[1] / v[2];
            return r;
          });

          this.setState({
            avgWaits: newData
          });
        },
        error: (err) => { console.log(err); },
        callbackName: 'jsonp' // Important !
      });

      // Get the card swipe acceptance rate
      JSONP({
        url: 'http://localhost:8080/api/1.0/',
        data: { Procedure: 'GetCardAcceptanceRate' },
        success: (data) => {
          this.setState({ rate: data.results[0].data[0][0] });
        },
        error: (err) => { console.log(err); },
        callbackName: 'jsonp' // Important !
      });
      
    }, 2000);

    // Update the data
    setInterval(() => {
        if (this.curSection > 4) this.curSection = 0;
        this.curSection = this.curSection + 1;
    }, 9000);


    }

  render() {    
    if (this.curSection === 1 ) {
        return (<div>
            <Pages1 num={this.state.num} rate={this.state.rate} busiestStations={this.state.busiestStations} avgWaits={this.state.avgWaits} theme={'car'} />
      </div>);
    } else if (this.curSection === 2) {
        return (
      <div>
            <Pages2 num={this.state.num} rate={this.state.rate} busiestStations={this.state.busiestStations} avgWaits={this.state.avgWaits} theme={'car'} />
      </div>);
    } else if (this.curSection === 3) {
        
        return (
      <div>
            <Pages3 num={this.state.num} rate={this.state.rate} busiestStations={this.state.busiestStations} avgWaits={this.state.avgWaits} theme={'car'} />
      </div>);
    } else if (this.curSection === 4) {
        return (
      <div>
            <Pages4 num={this.state.num} rate={this.state.rate} busiestStations={this.state.busiestStations} avgWaits={this.state.avgWaits} theme={'car'} />
      </div>);
    } else {
        return (
      <div>
            <Pages1 num={this.state.num} rate={this.state.rate} busiestStations={this.state.busiestStations} avgWaits={this.state.avgWaits} theme={'car'} />
      </div>);        
    }
  }
}

export default App;

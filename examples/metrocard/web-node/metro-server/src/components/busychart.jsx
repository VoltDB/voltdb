import React, { Component } from 'react';

import { XYPlot, XAxis, YAxis, HorizontalGridLines, VerticalBarSeries, Hint } from 'react-vis';

class BusiestStationsChart extends Component {
  constructor(props) {
    super(props);
    this.state = {
      value: null,
      color: null
    }
  }

  // BUG: animation={{ damping: 15, stiffness: 30 }} currently conflicting with the MouseOut handlers
  // https://github.com/uber/react-vis/issues/381
  render() {
    return (
      <div>
        <XYPlot
          margin={{ left: 100 }}
          xType={'ordinal'}
          width={window.innerWidth - 200}
          height={window.innerHeight - 200}>
          <XAxis title='Stations' />
          <YAxis title='Swipes' />
          <HorizontalGridLines />
          <VerticalBarSeries
            onValueMouseOver={(value, event) => {
              console.log(event);
              this.setState({ value: value });
            }}
            onValueMouseOut={event => {
              this.setState({ value: null });
            }}
            data={this.props.busiestStations}
          />
          {this.state.value ?
            <Hint value={this.state.value}>
              <BarHint value={this.state.value} />
            </Hint> :
            null
          }
        </XYPlot>
      </div>
    );
  }
}

class BarHint extends Component {
  render() {
    return (
      <div style={{ minWidth: '150px' }}>
        <div style={{
          borderBottom: '1px solid #313131',
          color: '#3a3a3a',
          fontWeight: 'bold',
          marginBottom: 5,
          paddingBottom: 5,
          textTransform: 'uppercase',
        }}>{this.props.value.x}</div>
        <div style={{ position: 'relative', height: '15px', width: '100%', color: '#3a3a3a' }}>
          <div style={{ position: 'absolute', left: 0 }}>Total Swipes</div>
          <div style={{ position: 'absolute', right: 0 }}>{this.props.value.y}</div>
        </div>
      </div>
    );
  }
}

export default BusiestStationsChart;
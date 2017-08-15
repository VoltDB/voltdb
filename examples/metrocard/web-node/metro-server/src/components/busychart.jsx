import React, { Component } from 'react';

import { XYPlot, XAxis, YAxis, HorizontalGridLines, VerticalBarSeries } from 'react-vis';

class BusiestStationsChart extends Component {
  render() {
    return (
      <div>
        <XYPlot
          margin={{left: 100}}
          xType={'ordinal'}
          width={window.innerWidth - 200}
          height={window.innerHeight - 200}>
          <XAxis title='Stations' />
          <YAxis title='Swipes' />
          <HorizontalGridLines />
          <VerticalBarSeries data={this.props.busiestStations} animation={{damping: 15, stiffness: 30}} />
        </XYPlot>
      </div>
    );
  }
}

export default BusiestStationsChart;
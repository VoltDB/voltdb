import React, { Component } from 'react';

import { XYPlot, XAxis, YAxis, HorizontalGridLines, VerticalBarSeries, LineSeries, LineMarkSeries } from 'react-vis';

class BusiestStationsChart extends Component {
  render() {
    return (
      <div>
        <XYPlot
          xType={'ordinal'}
          width={window.innerWidth - 50}
          height={window.innerHeight - 50}>
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
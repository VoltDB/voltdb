import React, { Component } from 'react';

import { XYPlot, XAxis, YAxis, HorizontalGridLines, LineSeries } from 'react-vis';

class AvgWaitsChart extends Component {
  render() {
    return (
      <div>
        <XYPlot
          xType={'ordinal'}
          width={window.innerWidth - 50}
          height={window.innerHeight - 50}>
          <XAxis />
          <YAxis />
          <HorizontalGridLines />
          <LineSeries color={'#e89b29'} data={this.props.avgWaits} animation={{damping: 15, stiffness: 30}} />
        </XYPlot>
      </div>
    );
  }
}

export default AvgWaitsChart;
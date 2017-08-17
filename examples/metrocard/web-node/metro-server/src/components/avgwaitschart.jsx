import React, { Component } from 'react';

import { XYPlot, XAxis, YAxis, HorizontalGridLines, LineSeries, MarkSeries, Hint } from 'react-vis';

class AvgWaitsChart extends Component {
  constructor(props) {
    super(props);
    this.state = {
      avgWaits: this.props.avgWaits,
      hovered: null,
      hoveredValue: null
    };
  }

  componentWillReceiveProps(nextProps) {
    this.setState({
      avgWaits: nextProps.avgWaits
    });
  }

  render() {
    var hovered = this.state.hovered;
    var hoveredValue = this.state.hoveredValue;

    return (
      <div>
        <XYPlot
          margin={{ left: 100 }}
          xType={'ordinal'}
          width={window.innerWidth - 150}
          height={window.innerHeight - 150}
          onMouseLeave={() => {
            this.setState({
              hovered: null
            });
          }}
        >
          <XAxis style={{
            text: { fontWeight: 600, fontSize: '55%' }
          }} />
          <YAxis />
          <HorizontalGridLines />
          <LineSeries
            onNearestX={(value, { event, innerX, innerY, index }) => {
              // record the nearest data point and set the state
              value.index = index;
              this.setState({
                hovered: true,
                hoveredValue: value
              });
            }}
            color={'#e89b29'} data={this.state.avgWaits} animation={{ damping: 15, stiffness: 30 }}
          />
          {this.state.hovered ?
            <MarkSeries
              data={[{
                x: hovered && hoveredValue.x,
                y: hovered && hoveredValue.y
              }]}
              color='rgba(226,57,27,0.7)'
              size='8px'
            /> : null
          }
          {this.state.hovered ?
            <Hint value={this.state.hoveredValue} orientation={'topleft'} >
              <MarkHint value={this.state.hoveredValue} />
            </Hint> :
            null
          }
        </XYPlot>
      </div>
    );
  }
}

class MarkHint extends Component {
  render() {
    return (
      <div style={{ minWidth: '200px', marginLeft: '40px', marginBottom: '40px' }}>
        <div style={{
          borderBottom: '1px solid #313131',
          color: '#3a3a3a',
          fontWeight: 'bold',
          marginBottom: 5,
          paddingBottom: 5,
          textTransform: 'uppercase',
        }}>{this.props.value.x}</div>
        <div style={{ position: 'relative', height: '15px', width: '100%', color: '#3a3a3a' }}>
          <div style={{ position: 'absolute', left: 0, width: '90%' }}>Average Waiting Time</div>
          <div style={{ position: 'absolute', left: '90%' }}>{this.props.value.y}</div>
        </div>
      </div>
    );
  }
}

export default AvgWaitsChart;
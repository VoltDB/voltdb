import React, { Component } from 'react';

import { XYPlot, XAxis, YAxis, HorizontalGridLines, VerticalBarSeries, Hint } from 'react-vis';

const highLightColor = '#fca832';
const barColor = '#12939a';

class BusiestStationsChart extends Component {
  constructor(props) {
    super(props);
    this.state = {
      busiestStations: this.props.busiestStations,
      value: null
    }
  }

  componentWillReceiveProps(nextProps) {
    let oldData = this.state.busiestStations.slice(0);
    if (oldData.length !== nextProps.busiestStations.length) {
      this.setState({
        busiestStations: nextProps.busiestStations
      });
      return;
    }

    // Keep the bar highlighted
    for (let i = 0; i < oldData.length; i++) {
      if (oldData[i].x === nextProps.busiestStations[i].x && oldData[i].color === highLightColor) {
        nextProps.busiestStations[i].color = highLightColor;
      } else {
        nextProps.busiestStations[i].color = barColor;
      }
    }

    this.setState({
      busiestStations: nextProps.busiestStations
    });
  }

  // BUG: animation={{ damping: 15, stiffness: 30 }} currently conflicting with the onValueMouseOut handlers
  // https://github.com/uber/react-vis/issues/381
  // onMouseLeave works fine
  render() {
    return (
      <div>
        <XYPlot
          margin={{ left: 100 }}
          xType={'ordinal'} // important !
          width={window.innerWidth - 150}
          height={window.innerHeight - 150}
          onMouseLeave={() => {
            var temp = this.state.busiestStations.slice(0);
            for (let i = 0; i < temp.length; i++) {
              temp[i].color = undefined;
            }
            this.setState({
              value: null,
              busiestStations: temp
            });
          }}
        >
          <XAxis title='Stations' style={{
            text: { fontWeight: 600, fontSize: '55%' }
          }} />
          <YAxis title='Swipes' />
          <HorizontalGridLines />
          <VerticalBarSeries
            colorType={'literal'} // important !
            onValueMouseOver={(value, event) => { // trigger the hover animation to show the info
              var temp = this.state.busiestStations.slice(0);
              for (let i = 0; i < temp.length; i++) {
                if (temp[i].x === value.x) {
                  temp[i].color = highLightColor;
                } else {
                  temp[i].color = barColor;
                }
              }
              this.setState({
                value: value,
                busiestStations: temp
              });
            }}
            data={this.state.busiestStations} animation={{ damping: 15, stiffness: 30 }}
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

// For showing the hint when hovering over a bar
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
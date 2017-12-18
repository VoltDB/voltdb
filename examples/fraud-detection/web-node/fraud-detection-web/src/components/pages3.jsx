import React, { Component } from 'react';

import { SectionsContainer, Section, Header } from 'react-fullpage';

// import NavbarInstance from './navbar.jsx';
import AvgWaitsChart from './avgwaitschart.jsx';

import logo from '../resources/logo.png';

import '../components-css/pages.css';

class Pages3 extends Component {
    
    render() {
    let options = {
      sectionClassName: 'section',
      scrollBar: false,
      navigation: true,
      verticalAlign: false,
      sectionPaddingTop: '50px',
      sectionPaddingBottom: '50px',
      arrowNavigation: true
    };

    return (
      <div>
        <Header>
          <a href="http://localhost:8080" className='nav-img'><img alt='Boom' src={logo} height="40" /></a>
          <a href="" className='nav-elem elem-right'>Fraud Monitoring System</a>
        </Header>
        <SectionsContainer className="container" {...options}>

          {/* The avg waiting time chart */}
          <Section class="custom-section" color="#E0E4CC">
            <div className='chart-1-title'>
              <p>Average Waiting Time</p>
              <p>per Station</p>
            </div>
            <div className='wrapper1'>
              <AvgWaitsChart avgWaits={this.props.avgWaits} />
            </div>
          </Section>

        </SectionsContainer>
      </div>
    );
  }
}

export default Pages3;

import React, { Component } from 'react';

import { SectionsContainer, Section, Header } from 'react-fullpage';

// import NavbarInstance from './navbar.jsx';
import OdometerInstance from './odometer.jsx';

import logo from '../resources/logo.png';

import '../components-css/pages.css';

class Pages1 extends Component {
    
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

          {/* The first odometer for showing # of card swipes */}
          <Section className="custom-section" verticalAlign="true" color="#eee0d3">
            <div style={{ fontSize: '5em' }}>
              <div className='wrapper1'>
                <div id="one">
                  <OdometerInstance value={this.props.num} theme={'car'} />
                </div>
                <div id="two">
                  Card Swipes
              </div>
              </div>
            </div>
          </Section>

        </SectionsContainer>
      </div>
    );
  }
}

export default Pages1;

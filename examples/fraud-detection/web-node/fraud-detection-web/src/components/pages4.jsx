import React, { Component } from 'react';

import { SectionsContainer, Section, Header } from 'react-fullpage';

// import NavbarInstance from './navbar.jsx';
import OdometerInstance from './odometer.jsx';

import logo from '../resources/logo.png';

import '../components-css/pages.css';

class Pages4 extends Component {
    
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

          {/* The frauds page */}
          <Section color="#111111">
            <div style={{ fontSize: '5em' }}>
              <div className='frauds-wrapper'>
                <div className='wrapper-odometer-2'>
                  <div>
                    <OdometerInstance format={'(,ddd).dd'} value={this.props.rate} theme={'digital'} />
                  </div>
                </div>
                <div className='frauds-text'>
                  ACCEPTANCE RATE
                </div>
              </div>
            </div>
          </Section>

        </SectionsContainer>
      </div>
    );
  }
}

export default Pages4;

import React, { Component } from 'react';

import { SectionsContainer, Section, Header, Footer } from 'react-fullpage';

// import NavbarInstance from './navbar.jsx';
import OdometerInstance from './odometer.jsx';
import BusiestStationsChart from './chart.jsx';

import logo from '../resources/logo.png';

import '../components-css/pages.css';

class Pages extends Component {
  render() {
    let options = {
      sectionClassName: 'section',
      anchors: ['sectionOne', 'sectionTwo', 'sectionThree'],
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
          <a href="" className='nav-img'><img alt='Boom' src={logo} height="40" /></a>
          <a href="#sectionOne" className='nav-elem'>Section One</a>
          <a href="#sectionTwo" className='nav-elem'>Section Two</a>
          <a href="#sectionThree" className='nav-elem'>Section Three</a>
          <a href="" className='nav-elem elem-right'>Metro Monitoring System</a>
        </Header>
        <Footer>
          <a href="">Dcoumentation</a>
          <a href="">Example Source</a>
          <a href="">About</a>
        </Footer>
        <SectionsContainer className="container" {...options}>

          {/* The first odometer for showing # of card swipes */}
          <Section className="custom-section" verticalAlign="true" color="#eee0d3">
            <section className='wrapper1'>
              <div id="one">
                <OdometerInstance value={this.props.num} theme={'car'} />
              </div>
              <div id="two">
                Card Swipes
              </div>
            </section>
          </Section>

          {/* The chart */}
          <Section color="#A7DBD8">
            <div className='chart-container'>
              <BusiestStationsChart busiestStations={this.props.busiestStations}/>
            </div>
          </Section>

          <Section color="#E0E4CC">Section 3</Section>

        </SectionsContainer>
      </div>
    );
  }
}

export default Pages;
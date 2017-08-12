import React, { Component } from 'react';

import { SectionsContainer, Section, Header, Footer } from 'react-fullpage';

// import NavbarInstance from './navbar.jsx';
import OdometerInstance from './odometer.jsx';

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
          <a href="" className='nav-elem nav-img'><img alt='Boom' src={logo} height="40" /></a>
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
          <Section className="custom-section" verticalAlign="true" color="#69D2E7">
            <OdometerInstance value={this.props.num} />
          </Section>
          <Section color="#A7DBD8">
            Section 2
          </Section>
          <Section color="#E0E4CC">Section 3</Section>
        </SectionsContainer>
      </div>
    );
  }
}

export default Pages;
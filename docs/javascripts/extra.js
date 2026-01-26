// Custom JavaScript for Excalibase REST Documentation

document.addEventListener('DOMContentLoaded', function() {
    // Add copy button functionality enhancement
    const codeBlocks = document.querySelectorAll('pre code');

    codeBlocks.forEach(function(block) {
        // Add line numbers for long code blocks
        const lines = block.textContent.split('\n');
        if (lines.length > 10) {
            block.classList.add('line-numbers');
        }
    });

    // Smooth scroll for anchor links
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function (e) {
            const href = this.getAttribute('href');
            if (href !== '#') {
                e.preventDefault();
                const target = document.querySelector(href);
                if (target) {
                    target.scrollIntoView({
                        behavior: 'smooth',
                        block: 'start'
                    });
                }
            }
        });
    });

    // Add external link indicators
    document.querySelectorAll('a[href^="http"]').forEach(link => {
        if (!link.hostname.includes('excalibase.github.io')) {
            link.setAttribute('target', '_blank');
            link.setAttribute('rel', 'noopener noreferrer');
        }
    });

    // Enhanced table responsiveness
    document.querySelectorAll('table').forEach(table => {
        if (!table.closest('.tabbed-set')) {
            const wrapper = document.createElement('div');
            wrapper.className = 'table-wrapper';
            table.parentNode.insertBefore(wrapper, table);
            wrapper.appendChild(table);
        }
    });

    // Code block language labels
    document.querySelectorAll('pre code[class*="language-"]').forEach(block => {
        const lang = block.className.match(/language-(\w+)/);
        if (lang && lang[1]) {
            const label = document.createElement('div');
            label.className = 'code-label';
            label.textContent = lang[1].toUpperCase();
            block.parentElement.insertBefore(label, block);
        }
    });
});

// Add search analytics (if Google Analytics is configured)
if (typeof gtag !== 'undefined') {
    document.querySelector('.md-search__input').addEventListener('keyup', function(e) {
        if (e.key === 'Enter' && this.value) {
            gtag('event', 'search', {
                'search_term': this.value
            });
        }
    });
}
